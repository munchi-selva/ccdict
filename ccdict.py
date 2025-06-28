#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Converts CC-CEDICT/CC-Canto dictionary data to an SQL format so it can be
easily queried.
The CC-CEDICT format is defined at https://CC-CEDICT.org/wiki/format:syntax.
CC-Canto, per https://cantonese.org/about.html, builds on CC-CEDICT by:
    1. Providing Jyutping renderings of CC-CEDICT entries whose definitions are
       also valid in Cantonese (CC-CEDICT-Canto below)
    2. Providing a separate listing of specifically Cantonese terms
The CC-Canto format augments CC-CEDICT entries with a Jyutping field.
"""

import ast                  # Abstract syntax tree helper, e.g. can convert a "list-like" string to a list
import cmd                  # Command line interpreter support
import click
import logging
import os
import re
import sqlite3
import sys
import time
from pprint import pformat, pprint   # Pretty printing module
from enum import auto, Enum, IntEnum, StrEnum

from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypeVar, Union

from click_shell import make_click_shell, Shell
from shell_with_default.shell_with_default import click_group_with_default, ClickShellWithDefault


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
    """
    Returns true if a string contains Han ideographs
    """

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
        """
        String representation of a dictionary search term
        """
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
        canto_tuples = parse_dict_entries(f"{self.dict_file_dir}/{CCCANTO_FILE}")
        db_cur.executemany("INSERT INTO cc_canto VALUES(?, ?, ?, ?, ?, ?)", canto_tuples)

        #
        # Import CC-CEDICT/-Canto data
        #
        cedict_tuples = parse_dict_entries(f"{self.dict_file_dir}/{CCCEDICT_FILE}")
        db_cur.executemany("INSERT INTO cc_cedict VALUES(?, ?, ?, ?, ?, ?)",
                cedict_tuples)

        cedict_canto_tuples = parse_dict_entries(f"{self.dict_file_dir}/{CCCEDICT_CANTO_FILE}")
        db_cur.executemany("INSERT INTO cc_cedict_canto VALUES(?, ?, ?, ?, ?, ?)",
            cedict_canto_tuples)

        print(f"Base cc_canto count: {row_count(db_cur, 'cc_canto')}")

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



###############################################################################
# Dictionary file parsing helper functions
###############################################################################

###############################################################################
def is_comment(dict_line):
    #
    # type (str) -> bool
    """
    Returns True if the dictionary file line is a comment

    :param  dict_line:  Dictionary file line
    :returns True if dict_line is a comment
    """
    return re.match("#", dict_line)
###############################################################################


###############################################################################
def parse_dict_line(dict_line):
    # type (str) -> Tuple
    """
    Parses a CC-CEDICT/CC-Canto/CC-CEDICT-Canto entry

    :param  dict_line:  Dictionary entry
    :returns a mapping between dictionary entry fields and values
    """
    m = re.match(DICT_PATT, dict_line)
    if m:
        groups = m.groupdict()
        eng_defs = groups[DE_FLD_ENGLISH].split("/") if groups[DE_FLD_ENGLISH] else [None]

        return [(groups[DE_FLD_TRAD],
                groups[DE_FLD_SIMP],
                groups[DE_FLD_PINYIN].lower() if groups[DE_FLD_PINYIN] else None,
                groups[DE_FLD_JYUTPING].lower() if groups[DE_FLD_JYUTPING] else None,
                eng.strip() if eng else None,
                groups[DE_FLD_COMMENT]) for eng in eng_defs]
    return None
###############################################################################


###############################################################################
def parse_dict_entries(dict_filename,
                       max_entries = -1):
    # type (str) -> List(Tuple)
    """
    Parses a file of CC-CEDICT/CC-Canto/CC-CEDICT-Canto entries as a list of
    tuples

    :param  dict_filename:  Name of the dictionary file
    :param  max_entries:    Maximum number of entries to parse, -1 => parse the
                            entire file
    :returns a list containing each entry parsed as a tuple
    """
    entries = list()
    entries_processed = 0
    with open(dict_filename) as dict_file:
        for dict_line in dict_file:
            if max_entries > 0 and entries_processed >= max_entries:
                break

            if not is_comment(dict_line):
                cccanto_tuples = parse_dict_line(dict_line)
                entries.extend(cccanto_tuples)
                entries_processed += 1
    return entries
###############################################################################




###############################################################################
# General sqlite helper functions
###############################################################################

###############################################################################
def regexp(pattern, field):
    # type (str, str) -> Bool
    """
    An implementation of the user function called by sqlite's REGEXP operator,
    allowing WHERE clause conditions that perform regular expression matching
    (See https://www.sqlite.org/lang_expr.html#regexp)

    :param  pattern:    Regular expression to be matched
    :param  field:      Field being regular expression tested
    :returns True if field matches pattern.
    """
    re_pattern = re.compile(pattern)
    return field and re_pattern.search(field) is not None
###############################################################################


###############################################################################
def table_exists(sqlcur, table_name):
    # type (Cursor, str -> int)
    """
    Checks if a table with the given name exists

    :param  sqlcur:     Cursor instance for running queries
    :param  table_name: Name of the table
    :returns True if the table exists
    """
    sqlcur.execute("SELECT COUNT(*) AS table_count FROM sqlite_master WHERE name = ?",
                   (table_name,))
    return (sqlcur.fetchone()[0] != 0)
###############################################################################


###############################################################################
def row_count(sqlcur, table_name):
    # type (Cursor, str -> int)
    """
    Returns the row count for a given table

    :param  sqlcur:     Cursor instance for running queries
    :param  table_name: Name of the table
    :returns the number of rows in the table
    """
    sqlcur.execute(f"SELECT COUNT(*) AS [{table_name} rows] FROM {table_name}")
    return sqlcur.fetchone()[0]
###############################################################################


###############################################################################
def show_query(sqlcur,
               query,
               as_dict = False):
    # type (str -> None)
    """
    Shows the results of a given query

    :param  sqlcur: Cursor instance for running queries
    :param  query:  The query
    :param as_dict: If True, displays each row returned as a dictionary
    :returns nothing
    """
    if as_dict:
        pprint([dict(row) for row in sqlcur.execute(query)])
    else:
        pprint([tuple(row) for row in sqlcur.execute(query)])
###############################################################################


###############################################################################
# Helper functions for processing/displaying dictionary search results
###############################################################################



###############################################################################
def str_to_bool(str):
    # type (str) -> bool
    """
    Converts a string to a boolean (if possible)

    :param  str:    A string
    :returns True/False/None depending on the value of the input string
    """
    if str.lower() in ("1", "t", "true"):
        return True
    elif str.lower() in ("0", "f", "false"):
        return False
    return None
###############################################################################


###############################################################################
# Shell command types enum
###############################################################################
CmdTknType = IntEnum("CmdTknType",  "CTT_NONE        \
                                     CTT_SEARCH_TERM \
                                     CTT_FIELD_LIST  \
                                     CTT_GENERAL     \
                                     CTT_COUNT",
                     start = -1)
###############################################################################


###############################################################################
# A class for defining a shell command token
###############################################################################
class CmdTkn(object):
    def __init__(self,
                 tkn_type,
                 cmd_start_patt,
                 cmd_end_patt,
                 inc_start_tkn,
                 inc_end_tkn = False):
        """
        Command token constructor

        :param  tkn_type:       the token type
        :param  cmd_start_patt: pattern that marks the token's tart
        :param  cmd_end_patt:   pattern than marks the token's end
        :param  inc_start_tkn:  If true, cmd_start_patt forms part of the command content
        :param  inc_end_tkn:    If True, cmd_end_patt forms part of the command content
        """
        self.tkn_type       = tkn_type
        self.cmd_start_patt = cmd_start_patt
        self.cmd_end_patt   = cmd_end_patt
        self.inc_start_tkn  = inc_start_tkn
        self.inc_end_tkn    = inc_end_tkn
    ###########################################################################


    ###########################################################################
    def __str__(self):
        """
        String representation of a command token definition
        """
        return "({}, {}, {}, {}, {})".format(self.tkn_type,
                                             self.cmd_start_patt,
                                             self.cmd_end_patt,
                                             self.inc_start_tkn,
                                             self.inc_end_tkn)
    ###########################################################################


    ###########################################################################
    def get_cmd_content(self,
                        tkn_src_str,
                        content_range_start,
                        content_range_end):
        """
        Returns the command content in the given range

        :param  tkn_src_str:            the command source string
        :param  content_range_start:    command content range start
        :param  content_range_end:      command content range end
        :returns the command content
        """
        return tkn_src_str[content_range_start:content_range_end].strip()
    ###########################################################################


    ###########################################################################
    def parse_tkn(self,
                  tkn_src_str,
                  tkn_start):
        """
        Parses the command token at a specified location in a source string

        :param  tkn_src_str:    the command source string
        :param  tkn_start:      token start index
        :returns the command content and the index of the token's end
        """
        tkn_end = len(tkn_src_str) - 1
        content_range_start = tkn_start + (0 if self.inc_start_tkn else 1)
        content_range_end   = len(tkn_src_str)

        #
        # Identify the end of the command token, and extract its content
        #
        end_tkn_match = re.search(self.cmd_end_patt, tkn_src_str[tkn_start+1:])
        if end_tkn_match:
            tkn_end = tkn_start + end_tkn_match.span()[1]
            if self.inc_end_tkn:
                content_range_end = tkn_end
            else:
                content_range_end = tkn_start + end_tkn_match.span()[0] + 1
        cmd_content = self.get_cmd_content(tkn_src_str, content_range_start, content_range_end)

        return cmd_content, tkn_end
###############################################################################


###############################################################################
# Command token subclass for parsing a dictionary search term
###############################################################################
class DictSearchTermCmdTkn(CmdTkn):
    def get_cmd_content(self,
                        tkn_src_str,
                        content_range_start,
                        content_range_end):
        cmd_content = None
        raw_content = super().get_cmd_content(tkn_src_str, content_range_start, content_range_end)
        search_val_patt = '(?P<search_val_complex>"(?P<search_val_quoted>[^"]+)")|(?P<search_val>[^\\s]+)'

        field_group_name = "search_field"
        value_group_name = "search_value"
        re_search_group_name = "search_with_re"

        search_term_patt = fr"(?P<{field_group_name}>\w+)\s+'?(?P<{value_group_name}>[^']+)'?(\s+(?P<{re_search_group_name}>\w+))?"

        search_val_match = re.match(search_term_patt, raw_content)
        if search_val_match is not None:
            match_groups = search_val_match.groupdict()
            search_field = match_groups[field_group_name]
            search_value = match_groups[value_group_name]
            search_with_re = match_groups[re_search_group_name] or None

            cmd_content = DictSearchTerm(search_value, search_field=eval(search_field), use_re=search_with_re)


#       search_val_match = re.match(search_val_patt, raw_content)
#       if search_val_match:
#           search_value = search_val_match["search_val_quoted"]
#           if not search_value:
#               search_value = search_val_match["search_val"]

#           search_term_args = {}
#           search_term_content = raw_content[search_val_match.span()[1]:].strip()
#           if search_term_content:
#               for search_term_elem in search_term_content.split():
#                   if str_to_bool(search_term_elem):
#                       search_term_args["use_re"] = str_to_bool(search_term_elem)
#                   elif search_term_elem in DE_FLDS_NAMES:
#                       search_term_args["search_field"] = eval(search_term_elem)
#           cmd_content = DictSearchTerm(search_value, **search_term_args)

        return cmd_content
###############################################################################


###############################################################################
# Command token subclass for parsing a dictionary field list
###############################################################################
class FldListCmdTkn(CmdTkn):
    def get_cmd_content(self,
                        tkn_src_str,
                        content_range_start,
                        content_range_end):
        raw_cmd_content = super().get_cmd_content(tkn_src_str, content_range_start, content_range_end)
        return [eval(field_name) for field_name in raw_cmd_content.split()]
###############################################################################


###############################################################################
# Command tokens for searching the dictionary
###############################################################################
SEARCH_CMD_TOKENS: List[CmdTkn] = \
[
    DictSearchTermCmdTkn(CmdTknType.CTT_SEARCH_TERM, r"\(", r"\)", False),
    FldListCmdTkn(CmdTknType.CTT_FIELD_LIST, r"\[", r"\]", False),
    CmdTkn(CmdTknType.CTT_GENERAL, r"\"", r"\"", False),
    CmdTkn(CmdTknType.CTT_GENERAL, r"[^\s]", r"[\s]", True)
]
###############################################################################


###############################################################################
def parse_dict_search_cmd(cmd: str,
                          cmd_tkn_defs: List[CmdTkn] = SEARCH_CMD_TOKENS) -> Dict:
    # type (str) -> Dict
    """
    Parses the components of a command for a dictionary search

    :param  cmd:    The command
    :returns a mapping between command component names and values
    """
    search_expr = None
    cmd_comps = dict()
    if cmd:
        #
        # Parse the command character by character...
        #
        tkn_start = 0
        while tkn_start < len(cmd):
            #
            # Identify the latest token's definition
            #
            tkn_def_matches = [defn for defn in cmd_tkn_defs if re.search(defn.cmd_start_patt, cmd[tkn_start])]
            tkn_def = tkn_def_matches[0] if len(tkn_def_matches) > 0 else None

            if tkn_def:
                cmd_content, tkn_end = tkn_def.parse_tkn(cmd, tkn_start)
                if tkn_def.tkn_type == CmdTknType.CTT_FIELD_LIST:
                    cmd_comps["fields"] = cmd_content
                elif tkn_def.tkn_type == CmdTknType.CTT_SEARCH_TERM:
                    search_expr = cmd_comps.get("search_expr", list())
                    if isinstance(search_expr, list):
                        search_expr.append(cmd_content)
                        cmd_comps["search_expr"] = search_expr
                else:
                    if str_to_bool(cmd_content) is not None:
                        search_expr = cmd_comps.get("search_expr", None)
                        if (not search_expr or isinstance(search_expr, str)) and not "use_re" in cmd_comps:
                            cmd_comps["use_re"] = str_to_bool(cmd_content)
                        elif not "flatten_pinyin" in cmd_comps:
                            cmd_comps["flatten_pinyin"] = str_to_bool(cmd_content)
                        elif not "compact" in cmd_comps:
                            cmd_comps["compact"] = str_to_bool(cmd_content)
                    else:
                        if cmd_content in DE_FLDS_NAMES:
                            cmd_comps["search_field"] = eval(cmd_content)
                        elif not search_expr:
                            if cmd[tkn_start] == '"' and not "use_re" in cmd_comps:
                                #
                                # Unless regular expression usage has been explicitly
                                # enabled/disabled, treat a quoted string as a
                                # regular expression that should be matched in full,
                                # i.e. search for ^<search_expr>$
                                #
                                cmd_comps["search_expr"] = "^" + cmd_content + "$"
                                cmd_comps["use_re"] = True
                            else:
                                cmd_comps["search_expr"] = cmd_content
                        elif not "indent_str" in cmd_comps:
                            cmd_comps["indent_str"] = cmd_content
                tkn_start = tkn_end + 1
            else:
                #
                # Advance position in the command
                #
                tkn_start += 1

    return cmd_comps
###############################################################################


OptDef = namedtuple("OptDef",
                    "name data_type default eval")
#defaults = (False,))


DictSearchOptId = IntEnum("DictSearchOptId",  "DSO_TRY_ALL \
                                               DSO_LAZY \
                                               DSO_FLATTEN_PINYIN \
                                               DSO_DO_RE_SEARCH \
                                               DSO_SEARCH_FLD \
                                               DSO_DISPLAY_FLDS \
                                               DSO_DISPLAY_FMT \
                                               DSO_DISPLAY_INDENT")

DictSearchOutputFormat = IntEnum("DictSearchOutputFormat",  "DSOF_ASCII \
                                                             DSOF_JSON")

@dataclass
class DictSearchOpt:
    T = TypeVar('T')

    id: DictSearchOptId
    type: TypeVar('T')
    default_value: type
    curr_val: Optional[type] = None


###############################################################################
# A class that implements an interactive shell for searching the dictionary
###############################################################################
class DictSearchCmd(cmd.Cmd):
    intro = "Cantonese dictionary search shell"
    prompt = "ccdict> "
    SET_CMD = "set"
    QUIT_CMD = "q"
    HELP_CMD = "?"
    dictionary = CantoDict(DICT_DB_FILENAME)

    std_opts = dict()
    std_opts["try_all_fields"]  = True
    std_opts["lazy_eval"]       = True

    DICT_OPT_SRCH_TRY_ALL   = "try_all_fields"
    DICT_OPT_SRCH_LAZY      = "lazy_eval"
    DICT_OPT_SRCH_FLATTEN   = "flatten_pinyin"
    DICT_OPT_SRCH_USE_RE    = "use_re"
    DICT_OPT_SRCH_FLD       = "search_field"
    DICT_OPT_DISP_COMPACT   = "compact"
    DICT_OPT_DISP_FLDS      = "fields"
    DICT_OPT_DISP_INDENT    = "indent_str"

    OPT_DEFS = [OptDef(DICT_OPT_SRCH_TRY_ALL,   "bool", True, False),
                OptDef(DICT_OPT_SRCH_LAZY,      "bool", True, False),
                OptDef(DICT_OPT_SRCH_FLATTEN,   "bool", True, False),
                OptDef(DICT_OPT_SRCH_USE_RE,    "bool", None, False),
                OptDef(DICT_OPT_SRCH_FLD,       "str",  None, True),
                OptDef(DICT_OPT_DISP_COMPACT,   "bool", False, False),
                OptDef(DICT_OPT_DISP_FLDS,      "list", ["DE_FLD_TRAD", "DE_FLD_CJCODE", "DE_FLD_JYUTPING", "DE_FLD_ENGLISH"], True),
                OptDef(DICT_OPT_DISP_INDENT,    "str",  "", False)]
    def __init__(self):
        super().__init__()
        self.settings = dict()
        for opt_def in DictSearchCmd.OPT_DEFS:
            self.settings[opt_def.name] = {"def": opt_def}

    def do_quit(self, arg):
        return True

    def do_help(self, arg):
        cmd_indent = "\t"
        opt_indent = "\t    "
        topic_field = "field"

        help_string = ""
        if arg == topic_field:
            help_string = "Valid dictionary entry fields\n" + \
                          cmd_indent + "DE_FLD_TRAD:\ttraditional Chinese\n" + \
                          cmd_indent + "DE_FLD_SIMP:\tsimplified Chinese\n" + \
                          cmd_indent + "DE_FLD_JYUTPING:\tJyutping transcription\n" + \
                          cmd_indent + "DE_FLD_PINYIN:\tPinyin transcription\n" + \
                          cmd_indent + "DE_FLD_ENGLISH:\tEnglish definition\n" + \
                          cmd_indent + "DE_FLD_CJCODE:\tChangJie code"
        else:
            help_string = "usage: " + \
                            cmd_indent + "search_term [search_field] [use_regex] [output_field1 ...]\n" + \
                            cmd_indent + "search_exp1 [search_exp2 ...] [output_field1 ...]\n" + \
                            opt_indent + "search_term: search string\n" + \
                            opt_indent + "search_field: field to match against search_term\n" + \
                            opt_indent + "search_exp1, search_exp2, etc.: search_val | (search_val search_field [use_regex])\n" + \
                          "Help topics: \n" + \
                            cmd_indent + "? field (search/output field options)"
        print(help_string)

    def do_set(self, arg):
        set_string = arg.strip() if arg else str()

        # Split the setting command into option name and values
        (opt_name, _, opt_val) = set_string.partition(" ")
        opt_val = opt_val.strip()

        # Find where the option name ends and any value change begins
        #name_end = set_string.index(" ") if (" " in set_string) else len(set_string)
        #opt_name = set_string[:name_end]
        #opt_val = set_string[name_end+1:].strip()

        opts_to_show = list()

        if not opt_name:
            opts_to_show = self.settings.keys()
        elif opt_name not in self.settings:
            print("Unrecognised option: {}".format(opt_name))
            return
        elif not opt_val:
            opts_to_show.append(opt_name)

        if opts_to_show:
            for opt_name in opts_to_show:
                # Print setting(s)
                opt_setting = self.settings[opt_name]
                print("{} = {}".format(opt_name, opt_setting.get("value", opt_setting["def"].default)))
            return

        opt_setting = self.settings[opt_name]
        opt_def = opt_setting["def"]
        opt_type = opt_def.data_type
        if opt_type == "bool":
            opt_val = str_to_bool(opt_val)
        elif opt_type == "list":
            opt_val = opt_val.split()

        canto_logger.log(logging.INFO, "BEFORE settings")
        pprint(self.settings)

        print("Setting: '{}'".format(opt_name))
        print("\tto: '{}'".format(opt_val))
        opt_setting["value"] = opt_val

        canto_logger.log(logging.INFO, "AFTER settings")
        pprint(self.settings)

    def do_search(self, arg):
        for opt_name in self.settings:
            if not opt_name in self.cmd_comps:

                opt_setting = self.settings[opt_name]
                opt_def = opt_setting["def"]
                opt_type = opt_def.data_type
                opt_val = opt_setting.get("value", opt_def.default)

                if opt_type == "list":
                    self.cmd_comps[opt_name] = list()
                    for val in opt_val:
                        if opt_def.eval:
                            val = eval(val)
                        self.cmd_comps[opt_name].append(val)
                elif opt_type == "str" and opt_def.eval:
                    if opt_val:
                        self.cmd_comps[opt_name] = eval(opt_val)
                else:
                    self.cmd_comps[opt_name] = opt_val

        search_expr = self.cmd_comps.pop("search_expr", None)
        print(f"Search expression is {search_expr}")
        if search_expr:
            self.dictionary.show_search(search_expr, **(self.cmd_comps))

    def precmd(self, line):
        line = line.strip()

        if line == self.QUIT_CMD:
            return "quit"

        line_tokens = line.split()

        if len(line_tokens) > 0:
            if line_tokens[0] == self.HELP_CMD or line_tokens[0] == self.SET_CMD:
                # Allow these commands to be passed in full to respective do_*() methods
                return line

        self.cmd_comps = parse_dict_search_cmd(line)
        return "search"
###############################################################################


###############################################################################
# An interactive shell for searching the dictionary
###############################################################################
@click_group_with_default(prompt="ccdict $ ", debug=False, custom_parser=parse_dict_search_cmd)
@click.pass_context
def ccdict_shell(ctx: click.Context):
    ctx.allow_extra_args = True
    ctx.ignore_unknown_options = True
    # Ensure that ctx.obj exists and is a dict
    ctx.ensure_object(dict)

    # Use ctx.obj to store the dictionary
    ctx.obj["dictionary"] = CantoDict(DICT_DB_FILENAME)
    initial_opts: List[DictSearchOpt] = [DictSearchOpt(id=DictSearchOptId.DSO_DISPLAY_FMT, type=DictSearchOutputFormat, default_value=DictSearchOutputFormat.DSOF_ASCII)]
    ctx.obj["opts"]: Dict[DictSearchOptId, DictSearchOpt] = {initial_opt.id: initial_opt for initial_opt in initial_opts}


@ccdict_shell.command(default=True)
@click.pass_context
@click.argument("search_term", type=str)

# Search behaviour options
@click.option("--all/--not-all", default=True, help="Search for matches for the search term across different search fields")
@click.option("--lazy/--not-lazy", default=True, help="Stop searching as soon as matches are found on one search field")
@click.option("--re/--no-re", default=None, help="Use regular expression searches")
#@click.option("-r", "--use-re", is_flag=True, default=None)
                          #(DICT_OPT_SRCH_FLD,       "str",  None, True),
                          #(DICT_OPT_DISP_COMPACT,   "bool", False, False),
                          #(DICT_OPT_DISP_INDENT,    "str",  "", False)]
@click.option("--flatten/--not-flatten", default=True, help="Treat two definitions for a DE_FLD_TRAD value even if their DE_FLD_PINYIN values differ")

# Search result display options
@click.option("-d", "--display-field",
              type=click.Choice(DE_FLDS_NAMES), multiple=True, default=["DE_FLD_TRAD", "DE_FLD_CJCODE", "DE_FLD_JYUTPING", "DE_FLD_ENGLISH"],
              help="Include the specified field in the search output")
@click.option("-c", "--compact", is_flag=True, default=False, help="Compact the search result to a single line")
@click.option("-f", "--output-format", type=click.Choice(["ASCII", "JSON"]), default="ASCII", help="Format of the search result")
def search(ctx: click.Context, search_term: str,
           all: bool,
           lazy: bool,
           re: Optional[bool],
           flatten: bool,
           display_field: List[str],
           compact: bool,
           output_format: str) -> List[str]: #None:
    cmd_comps =  parse_dict_search_cmd(search_term,
                                       cmd_tkn_defs = SEARCH_CMD_TOKENS)
    cmd_comps["try_all_fields"] = all
    cmd_comps["lazy_eval"] = lazy
    cmd_comps["use_re"] = re
    cmd_comps["flatten_pinyin"] = flatten
    cmd_comps["fields"] = [eval(field_name) for field_name in display_field]
    cmd_comps["output_format"] = CantoDict.DictOutputFormat.__getitem__(f"DOF_{output_format}")
    cmd_comps["compact"] = compact

    canto_logger.debug(f"Search command components: {pformat(cmd_comps)}")
    search_expr = cmd_comps.pop("search_expr", None)
    if search_expr:
        return ctx.obj["dictionary"].show_search(search_expr, **(cmd_comps))
    return []


###############################################################################


###############################################################################
def main():
    """
    """
    # Test search terms
    multi_jyutping_for_same_definition = "吼"

#   canto_dict = CantoDict(DICT_DB_FILENAME)
#   canto_dict.show_search("艦")

#   pre_lookup_time = time.perf_counter()
#   dict_lookup_result = canto_dict.get_formatted_search_results(multi_jyutping_for_same_definition,
#                                                                search_field=DE_FLD_TRAD,
#                                                                output_format=CantoDict.DictOutputFormat.DOF_JSON, fields=["jyutping", "english"])
#   post_lookup_time = time.perf_counter()
#   print(dict_lookup_result)
#   print(f"Lookup time without indexes: {post_lookup_time-pre_lookup_time:0.4f} seconds")

#   canto_dict = CantoDict(DICT_DB_FILENAME, build_indexes=True)
#   pre_lookup_time = time.perf_counter()
#   dict_lookup_result = canto_dict.get_formatted_search_results(multi_jyutping_for_same_definition,
#                                                                search_field=DE_FLD_TRAD,
#                                                                output_format=CantoDict.DictOutputFormat.DOF_JSON, fields=["jyutping", "english"])
#   post_lookup_time = time.perf_counter()
#   print(dict_lookup_result)
#   print(f"Lookup time with indexes: {post_lookup_time-pre_lookup_time:0.4f} seconds")

#   jyut_search_term = DictSearchTerm("jyun.", DE_FLD_JYUTPING, True)
#   eng_search_term = DictSearchTerm("surname", DE_FLD_ENGLISH, True)
#   canto_dict.show_search([jyut_search_term, eng_search_term], indent_str = "\t")
#   canto_dict.show_search("樂", fields = DE_FLDS)
#   canto_dict.show_search("樂", fields = DE_FLDS, flatten_pinyin = False, indent_str = "!!!!")
#   canto_dict.show_search("艦")

#   DictSearchCmd().cmdloop()

#   t = DictSearchTermCmdTkn(CmdTknType.CTT_SEARCH_TERM, r"\(", r"\)", False)
#   test_strs = [
#                   'DE_FLD_ENGLISH "Hello" true',
#                   'DE_FLD_ENGLISH "Hello"',
#                   'DE_FLD_ENGLISH   hello  ',
#                   'DE_FLD_ENGLISH',
#                   'DE_FLD_TRAD   "^我.*$"   true'
#               ]
#   for tkn_src_str in test_strs:
#       cmd_content = t.get_cmd_content(tkn_src_str, 0, len(tkn_src_str))
#       print(f"Searching based on '{tkn_src_str}'")
#       if cmd_content is not None:
#           canto_dict.show_search([cmd_content])

    ccdict_shell()
###############################################################################

if __name__ == "__main__":
    main()
