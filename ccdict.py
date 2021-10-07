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
DE_TRAD     = "traditional"
DE_SIMP     = "simplified"
DE_PINYIN   = "pinyin"
DE_JYUTPING = "jyutping"
DE_ENGLISH  = "english"
DE_COMMENT  = "comment"


DE_FIELDS   = [DE_TRAD, DE_SIMP, DE_PINYIN, DE_JYUTPING, DE_ENGLISH, DE_COMMENT]

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
TRAD_PATT       = "(?P<{}>[^\s]+)".format(DE_TRAD)
SIMP_PATT       = "(?P<{}>[^\s]+)".format(DE_SIMP)
PINYIN_PATT     = "\[(?P<{}>[^]]*)\]".format(DE_PINYIN)
JYUTPING_PATT   = "({{(?P<{}>[^}}]+)}})".format(DE_JYUTPING)
ENG_PATT        = "(/(?P<{}>.*)/)".format(DE_ENGLISH)
COMMENT_PATT    = "(#\s+(?P<{}>.*$))".format(DE_COMMENT)
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
    def __init__(self, search_value, search_field = DE_TRAD, use_re = False):
        """
        Dictionary search term constructor

        :param  search_value:   The search value
        :param  search_field:   The search field
        :param  use_re:         If True, search using regular expressions
        """
        self.search_value = search_value
        self.search_field = search_field
        self.use_re = use_re

    #
    # A read-only property that specifies which search operation to use
    #
    @property
    def search_op(self):
        return "REGEXP" if self.use_re else "="

    #
    # A read-only property that specifies the SQL query condition
    #
    @property
    def search_cond(self):
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
                                                             DE_TRAD,
                                                             DE_SIMP,
                                                             DE_PINYIN,
                                                             DE_JYUTPING,
                                                             DE_ENGLISH,
                                                             DE_COMMENT))

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
                                        c.{2} = cc.{2}".format(*DE_FIELDS)
        db_cur.execute(cedict_join_query)

        add_join_query = "INSERT INTO cc_canto \
                          SELECT c.{0}, c.{1}, c.{2}, c.{3}, c.{4}, c.{5} \
                          FROM   cedict_joined c LEFT JOIN cc_canto cc \
                                 ON c.{0} = cc.{0} AND \
                                    c.{1} = cc.{1} AND \
                                    c.{2} = cc.{2} AND \
                                    c.{4} = cc.{4} \
                          WHERE cc.{3} IS NULL".format(*DE_FIELDS)
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
                                WHERE  cc.{4} IS NULL".format(*DE_FIELDS)
        db_cur.execute(cedict_orphans_query)

        add_cedict_orphans_query = "INSERT INTO cc_canto \
                                    SELECT c.{0}, c.{1}, c.{2}, c.{3}, c.{4}, c.{5} \
                                    FROM   cedict_orphans c LEFT JOIN cc_canto cc \
                                           ON c.{0} = cc.{0} AND \
                                              c.{1} = cc.{1} AND \
                                              c.{2} = cc.{2} AND \
                                              c.{4} = cc.{4} \
                                    WHERE cc.{3} IS NULL".format(*DE_FIELDS)
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
                                      WHERE  c.{0} IS NULL".format(*DE_FIELDS)
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
                                          WHERE cc.{3} IS NULL".format(*DE_FIELDS)
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
                    search_terms,
                    flatten_pinyin = True):
        # type (List[DictSearchTerm], Bool) -> List[Dict]
        """
        Search for dictionary entries matching a combination of search terms

        :param search_expr:     List of search terms
        :param flatten_pinyin:  If True, flatten pinyin groupings in search results
        :returns a list of records (as dictionaries) matching the search terms
        """
        #
        # Extract the WHERE clause conditions from the search terms
        #
        where_clause = " AND ".join([search_expr.search_cond for search_expr in search_terms])
        where_values = tuple([search_expr.search_value for search_expr in search_terms])
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
                               ORDER BY {2}
                               GROUP BY {0}, {1})
                          SELECT {0}, {1}, {2}, json_group_array({3}) AS {3}
                          FROM   matching_defs
                          GROUP BY {0}, {1}, {2}
                          """
        canto_query = canto_query.format(DE_TRAD, DE_JYUTPING,
                                         DE_PINYIN, DE_ENGLISH,
                                         where_clause)
        self.db_cur.execute(canto_query, where_values)
        return [dict(row) for row in self.db_cur.fetchall()]
    ###########################################################################


    ###########################################################################
    def search(self,
               search_term,
               de_field         = DE_TRAD,
               use_re           = False,
               flatten_pinyin   = True):
        # type (str, str, Bool) -> None
        """
        Search for dictionary entries matching a single search term

        :param search_term:     Search term
        :param de_field:        The dictionary entry field to search
        :param use_re:          If True, treats search_term as a regular
                                expression
        :param flatten_pinyin:  If True, flatten pinyin groupings in search
                                results
        """
        search_expr = DictSearchTerm(search_term, de_field, use_re)
        return self.search_dict([search_expr])
    ###########################################################################


    ###########################################################################
    def show_search(self, search_expr, **kwargs):
        # type (str/List[DictSearchTerm]) -> None
        """
        Shows dictionary entries matching a search expression, which can be a
        single search string or a list of dictionary search terms

        :param search_expr: A search string or list of search terms
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
        if isinstance(search_expr, str):
            search_field    = kwargs.get("search_field", DE_TRAD)
            use_re          = kwargs.get("use_re", False)
            search_expr     = [DictSearchTerm(search_expr, search_field, use_re)]
        search_results = self.search_dict(search_expr)
        for search_result in search_results:
            print(format_search_result(search_result, **kwargs))
    ###########################################################################


    ###########################################################################
#    def show_single_search(self,
#                           search_term,
#                           **kwargs):
#        # type (str, str, Bool, Bool) -> None
#        """
#        Show dictionary entries matching a single search term
#
#        :param canto_dict       A Cantonese dictionary instance
#        :param search_term:     Search term
#        :optional/keyword arguments
#            de_field:       The dictionary entry field to search
#            use_re:         If True, treats search_term as a regular expression
#            flatten_pinyin: If True, flatten pinyin groupings in search results
#        """
#        de_field        = kwargs.get("de_field",        DE_TRAD)
#        use_re          = kwargs.get("use_re",          False)
#        self.show_search([DictSearchTerm(search_term, de_field, use_re)], **kwargs)
################################################################################
#



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
        eng_defs = groups[DE_ENGLISH].split("/") if groups[DE_ENGLISH] else [None]

        return [(groups[DE_TRAD],
                groups[DE_SIMP],
                groups[DE_PINYIN].lower() if groups[DE_PINYIN] else None,
                groups[DE_JYUTPING].lower() if groups[DE_JYUTPING] else None,
                eng.strip() if eng else None,
                groups[DE_COMMENT]) for eng in eng_defs]
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
    fields      = kwargs.get("fields",      [DE_TRAD, DE_JYUTPING, DE_ENGLISH])

    result_strings = list()
    decoder = json.JSONDecoder()

    for field in fields:
        if field in [DE_TRAD, DE_SIMP, DE_COMMENT]:
            result_strings.append(search_result.get(field, ""))
        elif field == DE_JYUTPING:
            jyutlist = decoder.decode(search_result[DE_JYUTPING])
            jyutstring = "[{}]".format(";".join(filter(None, jyutlist)))
            if DE_PINYIN in fields:
                # Queries may generate duplicate Pinyin results (although I've yet to see
                # this happen)... use a sledgehammer to get rid of them
                pinlist = list(set(search_result[DE_PINYIN].split(",")))
                pinstring = "({})".format(";".join(filter(None, pinlist)))
                jyutstring = "{} {}".format(jyutstring, pinstring)
            if not compact:
                jyutstring = "\t{}".format(jyutstring)
            result_strings.append(jyutstring)
        elif field == DE_ENGLISH:
            englist = decoder.decode(search_result[DE_ENGLISH])
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
def main():
    """
    """
    jyut_search_term = DictSearchTerm("jyun.", DE_JYUTPING, True)
    eng_search_term = DictSearchTerm("surname", DE_ENGLISH, True)
    canto_dict.show_search([jyut_search_term, eng_search_term], indent_str = "\t\t")
    canto_dict.show_search("阮", fields = DE_FIELDS, flatten_pinyin = False,
            indent_str = "!!!!")

###############################################################################

if __name__ == "__main__":
    canto_dict = CantoDict("ccdict.db")
    main()
