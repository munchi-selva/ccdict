#!/usr/bin/env python3
# -*- coding: utf-8 -*-

################################################################################
# Converts CC-CEDICT/CC-Canto dictionary data to an SQL format so it can be
# easily queried.
# The CC-CEDICT format is defined at https://CC-CEDICT.org/wiki/format:syntax.
# CC-Canto, per https://cantonese.org/about.html, builds on CC-CEDICT by:
#   1. Providing Jyutping renderings of CC-CEDICT entries whose definitions are
#      also valid in Cantonese (CC-CEDICT-Canto below)
#   2. Providing a separate listing of specifically Cantonese terms
# The CC-Canto format augments CC-CEDICT entries with a Jyutping field.
################################################################################
import json
import re
import sqlite3
from pprint import pprint   # Pretty printing module
from enum import IntEnum    # Backported to python 2.7 by https://pypi.org/project/enum34

###############################################################################
# Constants
###############################################################################

##################
# Dictionary files
##################
CC_DIR              = "/mnt/d/Documents/Computing/Dictionaries"
CCCEDICT_FILE       = "cedict_1_0_ts_utf-8_mdbg.txt"
CCCANTO_FILE        = "cccanto-webdist.txt"
CCCEDICT_CANTO_FILE = "cccedict-canto-readings-150923.txt"

###############################################################################
# Dictionary entry field names, used as SQL table column names, etc.
###############################################################################
DE_FLD_TRAD     = "traditional"
DE_FLD_SIMP     = "simplified"
DE_FLD_PINYIN   = "pinyin"
DE_FLD_JYUTPING = "jyutping"
DE_FLD_ENGLISH  = "english"
DE_FLD_COMMENT  = "comment"


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
TRAD_PATT       = "(?P<{}>[^\s]+)".format(DE_FLD_TRAD)
SIMP_PATT       = "(?P<{}>[^\s]+)".format(DE_FLD_SIMP)
PINYIN_PATT     = "\[(?P<{}>[^]]*)\]".format(DE_FLD_PINYIN)
JYUTPING_PATT   = "({{(?P<{}>[^}}]+)}})".format(DE_FLD_JYUTPING)
ENG_PATT        = "(/(?P<{}>.*)/)".format(DE_FLD_ENGLISH)
COMMENT_PATT    = "(#\s+(?P<{}>.*$))".format(DE_FLD_COMMENT)
DICT_PATT       = "{}\s+{}\s+{}\s+{}?\s*{}?\s*{}?".format(TRAD_PATT,
                                                          SIMP_PATT,
                                                          PINYIN_PATT,
                                                          JYUTPING_PATT,
                                                          ENG_PATT,
                                                          COMMENT_PATT)


###############################################################################
# A class to help with dictionary lookup
###############################################################################
class DictSearchTerm(object):
    def __init__(self, search_value, search_field = DE_FLD_TRAD, use_re = False):
        """
        Dictionary search term constructor

        :param  search_value:   The search value
        :param  search_field:   The search field
        :param  use_re:         If True, search using regular expressions
        """
        self.search_value = search_value
        self.search_field = search_field
        self.use_re = use_re
    ###########################################################################


    ###########################################################################
    def __str__(self):
        """
        String representation of a dictionary search term
        """
        return "DictSearchTerm({}, {}, {})".format(self.search_value, self.search_field, self.use_re)
    ###########################################################################


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
        return "{} {} ?".format(self.search_field, self.search_op)
###############################################################################




###############################################################################
# A class representing a searchable Cantonese dictionary
###############################################################################
class CantoDict(object):
    def __init__(self,
                 dict_db_filename  = ":memory:",
                 dict_file_dir     = CC_DIR):
        """
        Cantonese dictionary constructor

        :param  dict_db_filename:   sqlite3 database filename
        :param  dict_file_dir:      Directory hosting the dictionary source
                                    (text) files
        """
        self.db_filename    = dict_db_filename
        self.dict_file_dir  = dict_file_dir

        #
        # Set up database connection objects
        #
        self.db_con = sqlite3.connect(dict_db_filename)
        self.db_con.row_factory = sqlite3.Row           # Allow use of named columns in query results
        self.db_con.create_function("REGEXP", 2, regexp)
        self.db_cur = self.db_con.cursor()

        #
        # Load!
        #
        self.load_dict()
    ###########################################################################


    ###########################################################################
    def load_dict(self, force_reload = False, save_changes = True):
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
        # Check if the master dictionary table has already been created
        #
        count_query = "SELECT COUNT(*) AS table_count FROM sqlite_master WHERE name = ?"
        db_cur.execute(count_query, ("cc_canto",))
        cc_canto_count = db_cur.fetchone()[0]
        if cc_canto_count == 1 and not force_reload:
            return

        #
        # Clean out existing tables
        #
        for table_name in ["cc_cedict", "cc_canto", "cc_cedict_canto",
                "cedict_joined", "cedict_orphans", "cedict_canto_orphans"]:
            db_cur.execute("DROP TABLE IF EXISTS {}".format(table_name))

        for table_name in ["cc_cedict", "cc_canto", "cc_cedict_canto"]:
            db_cur.execute("CREATE TABLE {}({} text, \
                                            {} text, \
                                            {} text, \
                                            {} text, \
                                            {} text, \
                                            {} text)".format(table_name,
                                                             DE_FLD_TRAD,
                                                             DE_FLD_SIMP,
                                                             DE_FLD_PINYIN,
                                                             DE_FLD_JYUTPING,
                                                             DE_FLD_ENGLISH,
                                                             DE_FLD_COMMENT))

        #
        # Initiate core dictionary table with "pure" Cantonese data
        #
        canto_tuples = parse_dict_entries("{}/{}".format(self.dict_file_dir, CCCANTO_FILE))
        db_cur.executemany("INSERT INTO cc_canto VALUES(?, ?, ?, ?, ?, ?)", canto_tuples)

        #
        # Import CC-CEDICT/-Canto data
        #
        cedict_tuples = parse_dict_entries("{}/{}".format(self.dict_file_dir, CCCEDICT_FILE))
        db_cur.executemany("INSERT INTO cc_cedict VALUES(?, ?, ?, ?, ?, ?)",
                cedict_tuples)

        cedict_canto_tuples = parse_dict_entries("{}/{}".format(self.dict_file_dir, CCCEDICT_CANTO_FILE))
        db_cur.executemany("INSERT INTO cc_cedict_canto VALUES(?, ?, ?, ?, ?, ?)",
            cedict_canto_tuples)

        print("Base cc_canto count: {}".format(table_count(db_cur, "cc_canto")))

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

        print("After cedict join, count: {}".format(table_count(db_cur, "cc_canto")))

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

        print("After adding cedict orphans, count: {}".format(table_count(db_cur, "cc_canto")))

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

        print("After adding cedict canto orphans, count: {}".format(table_count(db_cur, "cc_canto")))

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
    def search_dict(self,
                    search_expr,
                    **kwargs):
        # type (List[DictSearchTerm], Bool) -> List[Dict]
        """
        Shows dictionary entries matching a search expression, which can be a
        single search string or a list of dictionary search terms

        :param  search_expr:    A search string or list of search terms
        :optional/keyword arguments
            search_field:   In single search mode, the search field to use
            use_re:         If True, treat the single search term as a regular
                            expression
            flatten_pinyin: If True, flatten pinyin groupings in search results
        :returns a list of records (as dictionaries) matching the search terms
        """
        flatten_pinyin = kwargs.get("flatten_pinyin", True)
        if isinstance(search_expr, str):
            search_field    = kwargs.get("search_field", DE_FLD_TRAD)
            use_re          = kwargs.get("use_re", False)
            search_expr     = [DictSearchTerm(search_expr, search_field, use_re)]
        else:
            for e in search_expr:
                print(e)

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
            canto_query = """
                          WITH matching_defs({0}, {1}, {2}, {3})
                          AS
                              (SELECT {0},
                                      json_group_array(DISTINCT({1})),
                                      group_concat(DISTINCT({2})),
                                      {3}
                               FROM   cc_canto
                               WHERE  {4}
                               GROUP BY {0}, {3})
                          SELECT {0}, {1},
                                 group_concat(DISTINCT({2})) AS {2},
                                 json_group_array({3}) AS {3}
                          FROM   matching_defs
                          GROUP BY {0}, {1}
                          """
        else:
            canto_query = """
                          WITH matching_defs({0}, {1}, {2}, {3})
                          AS
                              (SELECT {0},
                                      json_group_array(DISTINCT({1})),
                                      group_concat(DISTINCT({2})),
                                      {3}
                               FROM   cc_canto
                               WHERE  {4}
                               GROUP BY {0}, {3}
                               ORDER BY {2})
                          SELECT {0}, {1}, {2}, json_group_array({3}) AS {3}
                          FROM   matching_defs
                          GROUP BY {0}, {1}, {2}
                          """
        canto_query = canto_query.format(DE_FLD_TRAD, DE_FLD_JYUTPING,
                                         DE_FLD_PINYIN, DE_FLD_ENGLISH,
                                         where_clause)
        self.db_cur.execute(canto_query, where_values)
        return [dict(row) for row in self.db_cur.fetchall()]
    ###########################################################################


    ###########################################################################
    def show_search(self, search_expr, **kwargs):
        # type (str/List[DictSearchTerm]) -> None
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
        search_results = self.search_dict(search_expr, **kwargs)
        for search_result in search_results:
            print(format_search_result(search_result, **kwargs))
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
def table_count(sqlcur, table_name):
    # type (Cursor, str -> int)
    """
    Returns the row count for a given table

    :param  sqlcur:     Cursor instance for running queries
    :param  table_name: Name of the table
    :returns the number of rows in the table
    """
    sqlcur.execute("SELECT COUNT(*) AS [{0} rows] FROM {0}".format(table_name))
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
def format_search_result(search_result,
                         **kwargs):
    # type (Dict) -> None
    """
    Returns a dictionary search result as a formatted string.

    :param  search_result:  A dictionary search result
    :optional/keyword arguments
        indent_str: An indent string that prefixes each line of the formatted
                    search result
        compact:    If True, compact the search result to a single line
        fields:     The fields to include in the string

    :returns the search result as a string
    """

    #
    # Retrieve settings, or initialise them to defaults
    #
    indent_str  = kwargs.get("indent_str",  "")
    compact     = kwargs.get("compact",     False)
    fields      = kwargs.get("fields",      [DE_FLD_TRAD, DE_FLD_JYUTPING, DE_FLD_ENGLISH])

    result_strings = list()
    decoder = json.JSONDecoder()

    for field in fields:
        if field in [DE_FLD_TRAD, DE_FLD_SIMP, DE_FLD_COMMENT]:
            result_strings.append(search_result.get(field, ""))
        elif field == DE_FLD_JYUTPING:
            jyutlist = decoder.decode(search_result[DE_FLD_JYUTPING])
            jyutstring = "[{}]".format(";".join(filter(None, jyutlist)))
            if DE_FLD_PINYIN in fields:
                # Queries may generate duplicate Pinyin results (although I've yet to see
                # this happen)... use a sledgehammer to get rid of them
                pinlist = list(set(search_result[DE_FLD_PINYIN].split(",")))
                pinstring = "({})".format(";".join(filter(None, pinlist)))
                jyutstring = "{} {}".format(jyutstring, pinstring)
            if not compact:
                jyutstring = "\t{}".format(jyutstring)
            result_strings.append(jyutstring)
        elif field == DE_FLD_ENGLISH:
            englist = decoder.decode(search_result[DE_FLD_ENGLISH])
            if englist:
                if compact:
                    engsep = "; "
                    engstring = engsep.join(englist)
                    result_strings.append(engstring)
                else:
                    result_strings.extend(["\t{}".format(eng) for eng in englist])

    string_sep = " " if compact else "\n{}".format(indent_str)
    return "{}{}".format(indent_str, string_sep.join(result_strings))
###############################################################################


###############################################################################
def str_to_bool(str):
    # type (str) -> bool
    """
    Converts a string to a boolean (if possible)

    :param  str:    A string
    :returns True/False/None depending on the value of the input string
    """
    if str.lower() == "true":
        return True
    elif str.lower() == "false":
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
        cmd_comps = super().get_cmd_content(tkn_src_str, content_range_start, content_range_end).split()
        cmd_comps.reverse()
        if len(cmd_comps) >= 1:
            search_value = cmd_comps.pop()
            search_term_args = dict()
            for search_term_elem in cmd_comps:
                if str_to_bool(search_term_elem):
                    search_term_args["use_re"] = str_to_bool(search_term_elem)
                elif search_term_elem in DE_FLDS_NAMES:
                    search_term_args["search_field"] = eval(search_term_elem)
            cmd_content = DictSearchTerm(search_value, **search_term_args)
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
SEARCH_CMD_TOKENS = \
[
    DictSearchTermCmdTkn(CmdTknType.CTT_SEARCH_TERM, "\(", "\)", False),
    FldListCmdTkn(CmdTknType.CTT_FIELD_LIST, "\[", "\]", False),
    CmdTkn(CmdTknType.CTT_GENERAL, "\"", "\"", False),
    CmdTkn(CmdTknType.CTT_GENERAL, "[^\s]", "[\s]", True)
]
###############################################################################


###############################################################################
def parse_dict_search_cmd(cmd,
                          cmd_tkn_defs = SEARCH_CMD_TOKENS):
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


###############################################################################
def dict_search_shell(dictionary):
    PROMPT = "$ "
    CMD_QUIT = "q"

    while True:
        command = input(PROMPT)
        if command.lower() == CMD_QUIT:
            break
        cmd_comps = parse_dict_search_cmd(command)
        search_expr = cmd_comps.pop("search_expr", None)
        if search_expr:
            dictionary.show_search(search_expr, **cmd_comps)
###############################################################################


###############################################################################
def main():
    """
    """
    jyut_search_term = DictSearchTerm("jyun.", DE_FLD_JYUTPING, True)
    eng_search_term = DictSearchTerm("surname", DE_FLD_ENGLISH, True)
    canto_dict.show_search([jyut_search_term, eng_search_term], indent_str = "\t")
    canto_dict.show_search("樂", fields = DE_FLDS)
    canto_dict.show_search("樂", fields = DE_FLDS, flatten_pinyin = False, indent_str = "!!!!")

    dict_search_shell(canto_dict)


###############################################################################

if __name__ == "__main__":
    canto_dict = CantoDict("ccdict.db")
    main()
