#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Converts CC-CEDICT/CC-Canto dictionary data to query-friendly SQL format.

CC-CEDICT provides public domain Mandarin Chinese-English dictionary data,
per https://cc-cedict.org/wiki/start.
CC-Canto, per https://cantonese.org/about.html, builds on CC-CEDICT by:
    1. Providing Jyutping renderings of CC-CEDICT entries whose definitions are
       also valid in Cantonese (CC-CEDICT-Canto below)
    2. Providing a separate listing of specifically Cantonese terms
"""

import ast                  # Abstract syntax tree helper, e.g. can convert a "list-like" string to a list
import logging
import os
import re
import sqlite3
import sys
from enum import auto, Enum, IntEnum, StrEnum

from typing import Dict, List, Union

from cc_data_utils import parse_dict_file
from sql_utils import regexp, row_count, table_exists

canto_logger    = logging.getLogger(__name__)
console_handler = logging.StreamHandler(sys.stdout)
canto_logger.addHandler(console_handler)
canto_logger.setLevel(logging.INFO)

###############################################################################
# Constants
###############################################################################

##################
# Dictionary files
##################
CC_DIR              = "/mnt/d/src/cccanto"
CCCEDICT_FILE       = "cedict_1_0_ts_utf-8_mdbg.txt"
CCCANTO_FILE        = "cccanto-webdist.txt"
CCCEDICT_CANTO_FILE = "cccedict-canto-readings-150923.txt"
CJ_DIR              = "/mnt/d/Software_Development/Manuals,_Specs,_Tutorials/#Input_Methods/Cangjie"
CJV5_FILE           = "Cangjie_Version_5_Encodings_[ibus].txt"

#############################################################
# Default database file: in the same directory as this script
#############################################################
DICT_DB_DIR      = os.path.dirname(os.path.realpath(__file__))
DICT_DB_FILENAME = f"{DICT_DB_DIR}/ccdict.db"

###############################################################################
# Dictionary entry field names, used as SQL table column names, etc.
###############################################################################
# TODO: convert this to a StrEnum

class DictField(StrEnum):
    DF_TRAD     = "traditional"
    DF_SIMP     = "simplified"
    DF_PINYIN   = "pinyin"
    DF_JYUTPING = "jyutping"
    DF_ENGLISH  = "english"
    DF_COMMENT  = "comment"
    DF_CJCODE   = "cjcode"
    DF_CJCHAR   = "character"

DICT_FIELDS = list(DictField)
DICT_FIELD_NAMES = [str(dict_field) for dict_field in DICT_FIELDS]


DE_FLD_TRAD     = "traditional"
DE_FLD_SIMP     = "simplified"
DE_FLD_PINYIN   = "pinyin"
DE_FLD_JYUTPING = "jyutping"
DE_FLD_ENGLISH  = "english"
DE_FLD_COMMENT  = "comment"
DE_FLD_CJCODE   = "cjcode"
DE_FLD_CJCHAR   = "character"

DE_FLDS_NAMES   = [name for name in list(locals().keys()) if re.match("DE_FLD_", name)]
DE_FLDS         = [eval(fld_name) for fld_name in DE_FLDS_NAMES]

#
# CC-CEDICT format definition at https://CC-CEDICT.org/wiki/format:syntax.
# The CC-Canto format augments CC-CEDICT entries with a Jyutping field.
# CC-CEDICT format:
#   TRAD_CHIN SIMP_CHIN [PINYIN] /ENG 1/ENG 2/.../ENG N/
#
# CC-Canto format:
#   TRAD_CHIN SIMP_CHIN [PINYIN] {JYUTPING} /ENG 1/ENG 2/.../ENG N/
#
# CC-CEDICT-Canto format:
#   TRAD_CHIN SIMP_CHIN [PINYIN] {JYUTPING}
#
# Given the above formats, the following regular expressions allow
# CC-CEDICT, CC-Canto and CC-CEDICT-Canto lines to be parsed, yielding all
# fields of a complete CC-Canto entry.
#                   TRAD       SIMP        PINYIN     JYUTPING      ENG
TRAD_PATT       = fr"(?P<{DE_FLD_TRAD}>[^\s]+)"
SIMP_PATT       = fr"(?P<{DE_FLD_SIMP}>[^\s]+)"
PINYIN_PATT     = fr"\[(?P<{DE_FLD_PINYIN}>[^]]*)\]"
JYUTPING_PATT   = fr"({{(?P<{DE_FLD_JYUTPING}>[^}}]+)}})"
ENG_PATT        = fr"(/(?P<{DE_FLD_ENGLISH}>.*)/)"
COMMENT_PATT    = fr"(#\s+(?P<{DE_FLD_COMMENT}>.*$))"
DICT_PATT       = fr"{TRAD_PATT}\s+{SIMP_PATT}\s+{PINYIN_PATT}\s+{JYUTPING_PATT}?\s*{ENG_PATT}?\s*{COMMENT_PATT}?"

HAN_UNICODE_RANGES: list[range] = [range(0x4E00, 0x9FFF+1),     # CJK Unified Ideographs Common
                                   range(0x3400, 0x4DBF+1),     # CJK Unified Ideographs Extension A Rare
                                   range(0x20000, 0x2A6DF+1),   # CJK Unified Ideographs Extension B Rare, historic
                                   range(0x2A700, 0x2B73F+1),   # CJK Unified Ideographs Extension C Rare, historic
                                   range(0x2B740, 0x2B81F+1),   # CJK Unified Ideographs Extension D Uncommon, some in current use
                                   range(0x2B820, 0x2CEAF +1),  # CJK Unified Ideographs Extension E Rare, historic
                                   range(0x2CEB0, 0x2EBEF +1),  # CJK Unified Ideographs Extension F Rare, historic
                                   range(0x30000, 0x3134F +1),  # CJK Unified Ideographs Extension G Rare, historic
                                   range(0x31350, 0x323AF +1),  # CJK Unified Ideographs Extension H Rare, historic
                                   range(0xF900, 0xFAFF +1),    # CJK Compatibility Ideographs Duplicates, unifiable variants, corporate characters
                                   range(0x2F800, 0x2FA1F +1)   # CJK Compatibility Ideographs Supplement Unifiable variants
                                   ]

###############################################################################
# General helpers
###############################################################################
def contains_han(string: str) -> bool:
    """Returns true if a string contains Han ideographs."""
    return any(ord(letter) in han_range for han_range in HAN_UNICODE_RANGES for letter in string)


###############################################################################
# A class to help with dictionary lookup
###############################################################################
class DictSearchTerm(object):
    def __init__(self, search_value, search_field = DE_FLD_TRAD, use_re = None):
        """
        Dictionary search term constructor

        :param  search_value:   The search value
        :param  search_field:   The search field
        :param  use_re:         If True, search using regular expressions
        """
        self.search_value = search_value
        self.search_field = search_field
        self.use_re  = use_re
        if self.use_re is None:
            self.use_re = True if self.search_field == DE_FLD_ENGLISH else False
    ###########################################################################


    ###########################################################################
    def __str__(self):
        """String representation of a dictionary search term."""
        return f"DictSearchTerm({self.search_value}, {self.search_field}, {self.use_re})"
    ###########################################################################

    def __repr__(self):
        return self.__str__()


    ###########################################################################
    @property
    def search_op(self):
        """
        A read-only property that specifies which search operation to use
        """
        return "REGEXP" if self.use_re else "="
    ###########################################################################


    ###########################################################################
    @property
    def search_cond(self):
        """
        A read-only property that specifies the SQL query condition
        """
        return f"{self.search_field} {self.search_op} ?"
###############################################################################




class CantoDict(object):
    """
    Represents a searchable Cantonese dictionary
    """
    class DictOutputFormat(Enum):
        DOF_ASCII = auto()
        DOF_JSON = auto()

    def __init__(self,
                 dict_db_filename  = ":memory:",
                 dict_file_dir     = CC_DIR,
                 cj_file_dir       = CJ_DIR,
                 force_reload      = False):
        """
        Cantonese dictionary constructor

        :param  dict_db_filename:   sqlite3 database filename
        :param  dict_file_dir:      Directory hosting the dictionary source
                                    (text) files
        :param  cj_file_dir:        Directory hosting the Cangjie definition
                                    (text) file
        """
        self.db_filename    = dict_db_filename
        self.dict_file_dir  = dict_file_dir
        self.cj_file_dir    = cj_file_dir

        #
        # Set up database connection objects
        #
        self.db_con = sqlite3.connect(dict_db_filename)
        self.db_con.row_factory = sqlite3.Row           # Allow use of named columns in query results
        self.db_con.load_extension("/mnt/d/src/sqlite3_extensions/regexp")
        self.db_con.create_function("REGEXP", 2, regexp)
        self.db_cur = self.db_con.cursor()

        #
        # Load!
        #
        self.load_dict(force_reload=force_reload)
        self.load_canjie_defs(force_reload=force_reload)
    ###########################################################################


    ###########################################################################
    def load_dict(self, force_reload: bool = False, save_changes: bool = True):
        """
        Loads the dictionary data from the database file.

        If the database is empty, or a reload is required, loads the data from
        the dictionary text files.

        :param  force_reload:   If True, unconditionally (re)load dictionary
                                data from text files
        :param  save_changes:   If True, saves the results of a (re)load
        """
        # Copy of the cursor for convenience
        db_cur = self.db_cur


        #
        # No work is required unless a reload is forced or the master dictionary
        # table doesn't exist
        #
        if not(force_reload) and table_exists(db_cur, "cc_canto"):
            return

        #
        # Clean out existing tables
        #
        for table_name in ["cc_cedict", "cc_canto", "cc_cedict_canto",
                           "cedict_joined", "cedict_orphans", "cedict_canto_orphans"]:
            db_cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        for table_name in ["cc_cedict", "cc_canto", "cc_cedict_canto"]:
            db_cur.execute(f"CREATE TABLE {table_name}({DE_FLD_TRAD} text, \
                                                       {DE_FLD_SIMP} text, \
                                                       {DE_FLD_PINYIN} text, \
                                                       {DE_FLD_JYUTPING} text, \
                                                       {DE_FLD_ENGLISH} text, \
                                                       {DE_FLD_COMMENT} text)")

        #
        # Initiate core dictionary table with "pure" Cantonese data
        #
        canto_tuples = parse_dict_file(f"{self.dict_file_dir}/{CCCANTO_FILE}")
        db_cur.executemany("INSERT INTO cc_canto VALUES(?, ?, ?, ?, ?, ?)", canto_tuples)
        print(f"Base cc_canto count: {row_count(db_cur, 'cc_canto')}")

        #
        # Import CC-CEDICT/-Canto data
        #
        cedict_tuples = parse_dict_file(f"{self.dict_file_dir}/{CCCEDICT_FILE}")
        db_cur.executemany("INSERT INTO cc_cedict VALUES(?, ?, ?, ?, ?, ?)",
                cedict_tuples)
        print(f"Base cc_cedict count: {row_count(db_cur, 'cc_cedict')}")

        cedict_canto_tuples = parse_dict_file(f"{self.dict_file_dir}/{CCCEDICT_CANTO_FILE}")
        db_cur.executemany("INSERT INTO cc_cedict_canto VALUES(?, ?, ?, ?, ?, ?)",
            cedict_canto_tuples)
        print(f"Base cc_cedict_canto count: {row_count(db_cur, 'cc_cedict_canto')}")


        #
        # Join CC-CEDICT with CC-CEDICT-Canto entries based on traditional,
        # simplified and pinyin column values.
        # Add these records to the core table (if they aren't already there).
        #
        cedict_join_query = "CREATE TABLE cedict_joined AS \
                             SELECT c.{0}, c.{1}, c.{2}, cc.{3}, c.{4}, c.{5} \
                             FROM   cc_cedict c JOIN cc_cedict_canto cc \
                                    ON  c.{0} = cc.{0} AND \
                                        c.{1} = cc.{1} AND \
                                        c.{2} = cc.{2}".format(*DE_FLDS)
        db_cur.execute(cedict_join_query)

        add_join_query = "INSERT INTO cc_canto \
                          SELECT c.{0}, c.{1}, c.{2}, c.{3}, c.{4}, c.{5} \
                          FROM   cedict_joined c LEFT JOIN cc_canto cc \
                                 ON c.{0} = cc.{0} AND \
                                    c.{1} = cc.{1} AND \
                                    c.{2} = cc.{2} AND \
                                    c.{4} = cc.{4} \
                          WHERE cc.{3} IS NULL".format(*DE_FLDS)
        db_cur.execute(add_join_query)

        print(f"After cedict join, count: {row_count(db_cur, 'cc_canto')}")

        #
        # Identify CC-CEDICT orphans (entries with no CC-CEDICT-Canto match), and
        # add them to the core table
        #
        cedict_orphans_query = "CREATE TABLE cedict_orphans AS \
                                SELECT c.{0}, c.{1}, c.{2}, c.{3}, c.{4}, c.{5} \
                                FROM   cc_cedict c LEFT JOIN cc_cedict_canto cc \
                                ON     c.{0} = cc.{0} AND \
                                       c.{1} = cc.{1} AND \
                                       c.{2} = cc.{2} \
                                WHERE  cc.{4} IS NULL".format(*DE_FLDS)
        db_cur.execute(cedict_orphans_query)

        add_cedict_orphans_query = "INSERT INTO cc_canto \
                                    SELECT c.{0}, c.{1}, c.{2}, c.{3}, c.{4}, c.{5} \
                                    FROM   cedict_orphans c LEFT JOIN cc_canto cc \
                                           ON c.{0} = cc.{0} AND \
                                              c.{1} = cc.{1} AND \
                                              c.{2} = cc.{2} AND \
                                              c.{4} = cc.{4} \
                                    WHERE cc.{3} IS NULL".format(*DE_FLDS)
        db_cur.execute(add_cedict_orphans_query)

        print(f"After adding cedict orphans, count: {row_count(db_cur, 'cc_canto')}")

        #
        # Identify CC-CEDICT-Canto orphans and add them to the core table
        #
        cedict_canto_orphans_query = "CREATE TABLE cedict_canto_orphans AS \
                                      SELECT cc.{0}, cc.{1}, cc.{2}, cc.{3}, \
                                             cc.{4}, cc.{5} \
                                      FROM   cc_cedict_canto cc LEFT JOIN cc_cedict c \
                                      ON     c.{0} = cc.{0} AND \
                                             c.{1} = cc.{1} AND \
                                             c.{2} = cc.{2} \
                                      WHERE  c.{0} IS NULL".format(*DE_FLDS)
        db_cur.execute(cedict_canto_orphans_query)

        add_cedict_canto_orphans_query = "INSERT INTO cc_canto \
                                          SELECT c.{0}, c.{1}, c.{2}, c.{3}, \
                                                 c.{4}, c.{5} \
                                          FROM   cedict_canto_orphans c LEFT JOIN \
                                                 cc_canto cc \
                                                 ON c.{0} = cc.{0} AND \
                                                    c.{1} = cc.{1} AND \
                                                    c.{2} = cc.{2} AND \
                                                    c.{4} = cc.{4} \
                                          WHERE cc.{3} IS NULL".format(*DE_FLDS)
        db_cur.execute(add_cedict_canto_orphans_query)

        print(f"After adding cedict canto orphans, count: {row_count(db_cur, 'cc_canto')}")

        print("Creating indexes")
        trad_index_name = "cc_canto_trad"
        db_cur.execute(f"CREATE INDEX {trad_index_name} ON cc_canto({DE_FLD_TRAD})")

        simp_index_name = "cc_canto_simp"
        db_cur.execute(f"CREATE INDEX {simp_index_name} ON cc_canto({DE_FLD_SIMP})")

        jyutping_index_name = "cc_canto_jyutping"
        db_cur.execute(f"CREATE INDEX {jyutping_index_name} ON cc_canto({DE_FLD_JYUTPING})")

        # Needed: free text index for DE_FLD_ENGLISH

        if save_changes:
            self.save_dict()
    ###########################################################################


    ###########################################################################
    def load_canjie_defs(self, force_reload = False, save_changes = True):
        """
        Loads Cangjie definitions into the database as required.

        :param  force_reload:   If True, unconditionally (re)load Cangjie
                                definitions from text files
        :param  save_changes:   If True, saves the results of a (re)load
        """
        # Copy of the cursor for convenience
        db_cur = self.db_cur

        cj_def_filename = f"{self.cj_file_dir}/{CJV5_FILE}"
        cj_signs_table_name = "cj_sign_mappings"
        cj_dict_table_name = "cj_dict"

        #
        # Tags/delimiters for sections of interest in the CJ definition file
        #
        CJ_BEGIN_TAG    = "BEGIN_"
        CJ_END_TAG      = "END_"
        CJ_CODES_TAG    = "CHAR_PROMPTS_DEFINITION"
        CJ_DEFS_TAG     = "TABLE"

        #
        # (Re)load the table that maps alphabetical keys to CJ main signs
        #
        if force_reload or not(table_exists(db_cur, cj_signs_table_name)):
            print(f"Creating {cj_signs_table_name}")

            db_cur.execute(f"DROP TABLE IF EXISTS {cj_signs_table_name}")
            tbl_create_query = f"CREATE TABLE {cj_signs_table_name}(alpha_key text, \
                                                                    cj_sign text)"
            self.db_cur.execute(tbl_create_query)

            with open(cj_def_filename) as cj_file:
                cj_line = cj_file.readline()
                while cj_line and cj_line[:-1] != CJ_BEGIN_TAG + CJ_CODES_TAG:
                    cj_line = cj_file.readline()

                cj_line = cj_file.readline()
                while cj_line and cj_line[:-1] != CJ_END_TAG + CJ_CODES_TAG:
                    [alpha_key, cj_sign] = cj_line.split()
                    print(f"{alpha_key} {cj_sign}")
                    insert_query = f"INSERT INTO {cj_signs_table_name}(alpha_key, cj_sign) VALUES(?, ?)"
                    db_cur.execute(insert_query, (alpha_key, cj_sign))
                    cj_line = cj_file.readline()

        #
        # Cache alpha-CJ sign mappings so CJ sequences can be displayed sensibly
        #
        cj_keys = str()
        cj_signs = str()
        db_cur.execute(f"SELECT alpha_key, cj_sign FROM {cj_signs_table_name}")
        for row in db_cur.fetchall():
            cj_keys += dict(row)["alpha_key"]
            cj_signs += dict(row)["cj_sign"]
        self.cj_trans_table = "".maketrans(cj_keys, cj_signs)

        #
        # (Re)load the table that provides CJ mappings for individual characters
        #
        if force_reload or not(table_exists(db_cur, cj_dict_table_name)):
            print(f"Creating {cj_dict_table_name}")

            db_cur.execute(f"DROP TABLE IF EXISTS {cj_dict_table_name}")
            tbl_create_query = f"CREATE TABLE {cj_dict_table_name}({DE_FLD_CJCHAR} text, {DE_FLD_CJCODE} text)"
            db_cur.execute(tbl_create_query)

            with open(cj_def_filename) as cj_file:
                cj_line = cj_file.readline()
                while cj_line and cj_line[:-1] != CJ_BEGIN_TAG + CJ_DEFS_TAG:
                    cj_line = cj_file.readline()

                cj_line = cj_file.readline()
                while cj_line and cj_line[:-1] != CJ_END_TAG + CJ_DEFS_TAG:
                    [cj_code, character, _] = cj_line.split("\t")

                    cj_ins_query = f"INSERT INTO {cj_dict_table_name}({DE_FLD_CJCHAR}, {DE_FLD_CJCODE}) VALUES(?, ?)"
                    self.db_cur.execute(cj_ins_query, (character, cj_code))
                    cj_line = cj_file.readline()

        if save_changes:
            self.save_dict()
    ###########################################################################


    ###########################################################################
    def save_dict(self):
        """
        Saves the dictionary data to the nominated file.
        """
        self.db_con.commit()
    ###########################################################################


    ###########################################################################
    @staticmethod
    def is_multiple_value_field(field: str) -> bool:
        """
        Returns True if a dictionary entry can have multiple entries for the
        specified field.
        """
        return field in [DE_FLD_JYUTPING, DE_FLD_ENGLISH, DE_FLD_CJCODE]
    ###########################################################################


    ###########################################################################
    def search_dict(self,
                    search_expr: Union[str, List[DictSearchTerm]],
                    **kwargs) -> List[Dict]:
        """
        Retrieves dictionary entries matching a search expression, which can
        be a single search string or a list of dictionary search terms

        :param  search_expr:    A search string or list of search terms
        :optional/keyword arguments
            search_field:   In single search mode, the search field to use
            use_re:         If True, treat the single search term as a regular
                            expression
            try_all_fields: If True, in single search mode, search for matches
                            for the specified search term across different
                            search fields
            lazy_eval:      If True, in single search, all fields mode, use
                            lazy evaluation, i.e. stop searching as soon as
                            matches are found on one search field
        :returns a list of records (as dictionaries) matching the search terms
        """

        #
        # Retrieve optional argument values or defaults
        #
        try_all_fields  = kwargs.get("try_all_fields", False)
        lazy_eval       = kwargs.get("lazy_eval", True)
        use_re          = kwargs.get("use_re", None)

        search_expr_list    = []    # List of DictSearchTerm lists...
        dict_entries        = []    # Search results

        if isinstance(search_expr, str):
            search_field_list = list()
            if try_all_fields and "search_field" not in kwargs:
                # If Han ideographs appear, only search DE_FLD_TRAD/DE_FLD_SIMP
                if contains_han(search_expr):
                    search_field_list = [DE_FLD_TRAD, DE_FLD_SIMP]
                else:
                    search_field_list = [DE_FLD_JYUTPING, DE_FLD_ENGLISH, DE_FLD_CJCODE]
            else:
                search_field_list = [kwargs.get("search_field", DE_FLD_TRAD)]

            for search_field in search_field_list:
                search_expr_list.append([(DictSearchTerm(search_expr, search_field, use_re))])
        else:
            if use_re is not None:
                for se in search_expr:
                    se.use_re = use_re
            search_expr_list = [search_expr]

        #
        # Iterate over search expressions as required
        #
        for search_expr in search_expr_list:
            dict_entries.extend(self.apply_search_expr(search_expr, **kwargs))
            if lazy_eval and len(dict_entries) != 0:
                break

        for dict_entry in dict_entries:
            for field in dict_entry:
                # Convert fields that can have multiple values per entry to lists
                if CantoDict.is_multiple_value_field(field):
                    #print(f"Current field value = {dict_entry[field]}")
                    dict_entry[field] = ast.literal_eval(dict_entry[field])

        return dict_entries
    ###########################################################################



    ###########################################################################
    def apply_search_expr(self,
                          search_expr,
                          **kwargs):
        # type (List[DictSearchTerm], Bool) -> List[Dict]
        """
        Retrieves dictionary entries matching a search expression, which is
        formatted as a list of dictionary search terms

        :param  search_expr:    A list of search terms
        :optional/keyword arguments
            flatten_pinyin: If True, flatten pinyin groupings in search results
        :returns a list of records (as dictionaries) matching the search terms
        """
        flatten_pinyin  = kwargs.get("flatten_pinyin", True)

        #
        # Extract the WHERE clause conditions from the search terms
        #
        where_clause = " AND ".join([search_term.search_cond for search_term in search_expr])
        where_values = tuple([search_term.search_value for search_term in search_expr])

        #
        # Build a two-stage query that groups records matching the search terms
        # according to Jyutping and English definition
        #
        canto_query = str()
        if flatten_pinyin:
            canto_query = f"""
                          WITH matching_defs({DE_FLD_TRAD}, {DE_FLD_JYUTPING}, {DE_FLD_PINYIN}, {DE_FLD_ENGLISH}, {DE_FLD_CJCODE}, {DE_FLD_SIMP})
                          AS
                              (SELECT {DE_FLD_TRAD},
                                      regexp_replace(json_group_array(DISTINCT({DE_FLD_JYUTPING})), 'null,?', ''),
                                      group_concat(DISTINCT({DE_FLD_PINYIN})),
                                      {DE_FLD_ENGLISH},
                                      cj_dict.{DE_FLD_CJCODE},
                                      {DE_FLD_SIMP}
                               FROM   cc_canto LEFT JOIN
                                      cj_dict ON cc_canto.{DE_FLD_TRAD} = cj_dict.{DE_FLD_CJCHAR}
                               WHERE  {where_clause}
                               GROUP BY {DE_FLD_TRAD}, {DE_FLD_ENGLISH}, {DE_FLD_CJCODE}, {DE_FLD_SIMP})
                          SELECT {DE_FLD_TRAD}, {DE_FLD_JYUTPING}, {DE_FLD_SIMP},
                                 group_concat(DISTINCT({DE_FLD_PINYIN})) AS {DE_FLD_PINYIN},
                                 regexp_replace(json_group_array(DISTINCT({DE_FLD_ENGLISH})), 'null,?', '') AS {DE_FLD_ENGLISH},
                                 regexp_replace(json_group_array(DISTINCT({DE_FLD_CJCODE})), 'null,?', '') AS {DE_FLD_CJCODE}
                          FROM   matching_defs
                          GROUP BY {DE_FLD_TRAD}, {DE_FLD_JYUTPING}, {DE_FLD_SIMP}
                          """
        else:
            canto_query = f"""
                          WITH matching_defs({DE_FLD_TRAD}, {DE_FLD_JYUTPING}, {DE_FLD_PINYIN}, {DE_FLD_ENGLISH}, {DE_FLD_CJCODE})
                          AS
                              (SELECT {DE_FLD_TRAD},
                                      regexp_replace(json_group_array(DISTINCT({DE_FLD_JYUTPING})), 'null,?', ''),
                                      group_concat(DISTINCT({DE_FLD_PINYIN})),
                                      {DE_FLD_ENGLISH},
                                      group_concat(DISTINCT({DE_FLD_CJCODE}))
                               FROM   cc_canto LEFT JOIN
                                      cj_dict ON cc_canto.{DE_FLD_TRAD} = cj_dict.{DE_FLD_CJCHAR}
                               WHERE  {where_clause}
                               GROUP BY {DE_FLD_TRAD}, {DE_FLD_ENGLISH}, {DE_FLD_CJCODE}
                               ORDER BY {DE_FLD_PINYIN})
                          SELECT {DE_FLD_TRAD}, {DE_FLD_JYUTPING}, {DE_FLD_PINYIN},
                                 regexp_replace(json_group_array(DISTINCT({DE_FLD_ENGLISH})), 'null,?', '') AS {DE_FLD_ENGLISH},
                                 regexp_replace(json_group_array(DISTINCT({DE_FLD_CJCODE})), 'null,?', '') AS {DE_FLD_CJCODE}
                          FROM   matching_defs
                          GROUP BY {DE_FLD_TRAD}, {DE_FLD_JYUTPING}, {DE_FLD_PINYIN}
                          """
        self.db_cur.execute(canto_query, where_values)
        return [dict(row) for row in self.db_cur.fetchall()]
    ###########################################################################


    ###########################################################################
    def translate_cj_seq(self,
                         cj_seq: str) -> str:
        """
        Returns the Cangjie signs corresponding to a given key sequence.

        :param  cj_seq:     An alphabetical string
        :returns the corresponding Cangjie signs
        """
        return cj_seq.translate(self.cj_trans_table)
    ###########################################################################


    ###########################################################################
    def get_formatted_search_results(self,
                                     search_expr: Union[str, List[DictSearchTerm]],
                                     **kwargs) -> List[str]: #None:
        """
        Retrieves formatted dictionary entries matching a search expression,
        which can be a single search string or a list of dictionary search terms

        :param  search_expr:    A search string or list of search terms
        :optional/keyword arguments
            search_field:   In single search mode, the search field to use
            use_re:         If True, treat the single search term as a regular
                            expression
            flatten_pinyin: If True, flatten pinyin groupings in search results
            indent_str:     An indent string that prefixes each line of the
                            formatted search result
            compact:        If True, compact the search result to a single line
            fields:         The fields to include in the string
        :returns nothing
        """
        return [self.format_search_result(search_result, **kwargs) for search_result in self.search_dict(search_expr, **kwargs)]
    ###########################################################################


    ###########################################################################
    def show_search(self,
                    search_expr: Union[str, List[DictSearchTerm]],
                    **kwargs) -> None:
        """
        Shows dictionary entries matching a search expression, which can be a
        single search string or a list of dictionary search terms

        :param  search_expr:    A search string or list of search terms
        :optional/keyword arguments
            search_field:   In single search mode, the search field to use
            use_re:         If True, treat the single search term as a regular
                            expression
            flatten_pinyin: If True, flatten pinyin groupings in search results
            indent_str:     An indent string that prefixes each line of the
                            formatted search result
            compact:        If True, compact the search result to a single line
            fields:         The fields to include in the string
        :returns nothing
        """
        for formatted_search_result in self.get_formatted_search_results(search_expr, **kwargs):
            print(formatted_search_result)
    ###########################################################################


    ###########################################################################
    def format_search_result(self,
                             search_result: Dict[str, str],
                             **kwargs) -> str:
        """
        Returns a dictionary search result as a formatted string.

        :param  search_result:  A dictionary search result, a mapping of field
                                names to values
        :optional/keyword arguments
            indent_str:     An indent string that prefixes each line of the
                            formatted search result
            output_format:  Format of the search result
            compact:        If True, compact the search result to a single line
            fields:         The fields to include in the string

        :returns the search result as a string
        """

        #
        # Retrieve settings, or initialise them to defaults
        #
        indent_str      = kwargs.get("indent_str",      "")
        output_format   = kwargs.get("output_format",   CantoDict.DictOutputFormat.DOF_ASCII)
        compact         = kwargs.get("compact",         False)
        fields          = kwargs.get("fields",          [DE_FLD_TRAD, DE_FLD_CJCODE, DE_FLD_JYUTPING, DE_FLD_ENGLISH])


        if output_format == CantoDict.DictOutputFormat.DOF_JSON:
            # Grab a copy of the required fields
            filtered_dict_entry = {field:value for field, value in search_result.items() if field in fields}
            logging.debug(f"Filtered dictionary entry = {filtered_dict_entry}")

            # Compact fields that have multiple values per entry
            for field, value in filtered_dict_entry.items():
                if isinstance(value, List):
                    field_sep = ";" if field == DE_FLD_JYUTPING else "; "
                    filtered_dict_entry[field] = field_sep.join(value)
            return str(filtered_dict_entry)
        elif output_format == CantoDict.DictOutputFormat.DOF_ASCII:
            result_strings = list()
            chinese_fld_idx = -1

            for field in fields:
                if field in [DE_FLD_TRAD, DE_FLD_SIMP]:
                    field_val = search_result.get(field, "")
                    if chinese_fld_idx == -1:
                        result_strings.append(field_val)
                        chinese_fld_idx = len(result_strings) - 1
                    else:
                        result_strings[chinese_fld_idx] += f" <=> {field_val}"
                elif field == DE_FLD_COMMENT:
                    result_strings.append(search_result.get(field, ""))
                elif field == DE_FLD_JYUTPING:
                    jyutstring = f"[{';'.join(search_result[field])}]"

                    if DE_FLD_PINYIN in fields:
                        # Queries may generate duplicate Pinyin results (although I've yet to see
                        # this happen)... use a sledgehammer to get rid of them
                        pinlist = list(set(search_result[DE_FLD_PINYIN].split(",")))
                        pinstring = "({})".format(";".join(filter(None, pinlist)))
                        jyutstring = f"{jyutstring} {pinstring}"
                    if not compact:
                        jyutstring = f"\t{jyutstring}"
                    result_strings.append(jyutstring)
                elif field == DE_FLD_ENGLISH:
                    if search_result[field]:
                        if compact:
                            fldsep = "; "
                            fldstring = fldsep.join(search_result[field])
                            result_strings.append(fldstring)
                        else:
                            result_strings.extend([f"\t{fld}" for fld in search_result[field]])
                elif field == DE_FLD_CJCODE:
                    cjlist = search_result[field]
                    if cjlist and cjlist[0]:
                        cj_strings = [self.translate_cj_seq(cjseq) for cjseq in cjlist]
                        result_strings.append("\t{}".format(" ".join(cj_strings)))

            string_sep = " " if compact else f"\n{indent_str}"
            return "{}{}".format(indent_str, string_sep.join(result_strings))

        return ""
###############################################################################
