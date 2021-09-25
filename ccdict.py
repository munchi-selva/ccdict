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
# sqlite helper functions
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
def table_count(table_name):
    # type (str -> int)
    """
    Returns the row count for a given table

    :param table_name:  The name of the table
    :returns the number of rows in the table
    """
    cursor = sqlcon.cursor()
    cursor.execute("SELECT COUNT(*) AS [{0} rows] FROM {0}".format(table_name))
    return cursor.fetchone()[0]
###############################################################################


###############################################################################
def show_query(query,
               as_dict = False):
    # type (str -> None)
    """
    Shows the results of a given query

    :param query:   The query
    :param as_dict: If True, displays each row returned as a dictionary
    :returns nothing
    """
    if as_dict:
        pprint([dict(row) for row in sqlcon.execute(query)])
    else:
        pprint([tuple(row) for row in sqlcon.execute(query)])
###############################################################################


###############################################################################
def create_dict():
    """
    Create and populates the dictionary table(s)
    """
    print("Initialising dictionary tables")
    for table_name in ["cc_cedict", "cc_canto", "cc_cedict_canto"]:
        sqlcon.execute("CREATE TABLE {}({} text, \
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
    canto_tuples = parse_dict_entries("{}/{}".format(CC_DIR, CCCANTO_FILE))
    sqlcon.executemany("INSERT INTO cc_canto VALUES(?, ?, ?, ?, ?, ?)", canto_tuples)

    #
    # Import CC-CEDICT/-Canto data
    #
    cedict_tuples = parse_dict_entries("{}/{}".format(CC_DIR, CCCEDICT_FILE))
    sqlcon.executemany("INSERT INTO cc_cedict VALUES(?, ?, ?, ?, ?, ?)",
            cedict_tuples)

    cedict_canto_tuples = parse_dict_entries("{}/{}".format(CC_DIR, CCCEDICT_CANTO_FILE))
    sqlcon.executemany("INSERT INTO cc_cedict_canto VALUES(?, ?, ?, ?, ?, ?)",
        cedict_canto_tuples)

    print("Base cc_canto count: {}".format(table_count("cc_canto")))

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
    sqlcon.execute(cedict_join_query)

    add_join_query = "INSERT INTO cc_canto \
                      SELECT c.{0}, c.{1}, c.{2}, c.{3}, c.{4}, c.{5} \
                      FROM   cedict_joined c LEFT JOIN cc_canto cc \
                             ON c.{0} = cc.{0} AND \
                                c.{1} = cc.{1} AND \
                                c.{2} = cc.{2} AND \
                                c.{4} = cc.{4} \
                      WHERE cc.{3} IS NULL".format(*DE_FIELDS)
    sqlcon.execute(add_join_query)

    print("After cedict join, count: {}".format(table_count("cc_canto")))

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
    sqlcon.execute(cedict_orphans_query)

    add_cedict_orphans_query = "INSERT INTO cc_canto \
                                SELECT c.{0}, c.{1}, c.{2}, c.{3}, c.{4}, c.{5} \
                                FROM   cedict_orphans c LEFT JOIN cc_canto cc \
                                       ON c.{0} = cc.{0} AND \
                                          c.{1} = cc.{1} AND \
                                          c.{2} = cc.{2} AND \
                                          c.{4} = cc.{4} \
                                WHERE cc.{3} IS NULL".format(*DE_FIELDS)
    sqlcon.execute(add_cedict_orphans_query)

    print("After adding cedict orphans, count: {}".format(table_count("cc_canto")))

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
    sqlcon.execute(cedict_canto_orphans_query)

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
    sqlcon.execute(add_cedict_canto_orphans_query)

    print("After adding cedict canto orphans, count: {}".format(table_count("cc_canto")))
###############################################################################


###############################################################################
def search_dict(search_exprs,
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
    where_clause = " AND ".join([search_expr.search_cond for search_expr in search_exprs])
    where_values = tuple([search_expr.search_value for search_expr in search_exprs])
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
    sqlcur.execute(canto_query, where_values)
    return [dict(row) for row in sqlcur.fetchall()]
################################################################################


###############################################################################
def search(search_term,
           de_field         = DE_TRAD,
           use_re           = False,
           flatten_pinyin   = True):
    # type (str, str, Bool) -> None
    """
    Search for dictionary entries matching a single search term

    :param search_term:     Search term
    :param de_field:        The dictionary entry field to search
    :param use_re:          If True, treats search_term as a regular expression
    :param flatten_pinyin:  If True, flatten pinyin groupings in search results
    """
    search_expr = DictSearchTerm(search_term, de_field, use_re)
    return search_dict([search_expr])
################################################################################


###############################################################################
def show_search_result(search_result):
    # type (Dict) -> None
    """
    Displays a dictionary search result

    :param search_result: List of search results
    :returns nothing
    """
    decoder = json.JSONDecoder()

    jyutlist = decoder.decode(search_result[DE_JYUTPING])
    jyutstring = ";".join(filter(None, jyutlist))

    # Queries may generate duplicate Pinyin results (although I've yet to see
    # this happen)... use a sledgehammer to get rid of them
    pinlist = list(set(search_result[DE_PINYIN].split(",")))
    pinstring = ";".join(filter(None, pinlist))

    englist = decoder.decode(search_result[DE_ENGLISH])
    print("{}\n\t[{}] ({})".format(search_result[DE_TRAD], jyutstring, pinstring))
    if englist:
        for eng in englist:
            print("\t{}".format(eng))
###############################################################################


###############################################################################
def show_search(search_exprs):
    # type (List[DictSearchTerm]) -> None
    """
    Shows dictionary entries matching a combination of search terms

    :param search_expr: List of search terms
    :returns nothing
    """
    search_results = search_dict(search_exprs)
    for search_result in search_results:
        show_search_result(search_result)
###############################################################################


###############################################################################
def show_single_search(search_term,
                       de_field         = DE_TRAD,
                       use_re           = False,
                       flatten_pinyin   = True):
    # type (str, str, Bool, Bool) -> None
    """
    Show dictionary entries matching a single search term

    :param search_term:     Search term
    :param de_field:        The dictionary entry field to search
    :param use_re:          If True, treats search_term as a regular expression
    :param flatten_pinyin:  If True, flatten pinyin groupings in search results
    """
    search_results = search(search_term, de_field, use_re, flatten_pinyin)
    for search_result in search_results:
        show_search_result(search_result)
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
def parse_dict_line(dict_line):
    # type (str) -> Tuple
    """
    Parses a CC-CEDICT/CC-Canto/CC-CEDICT-Canto entry

    :param  dict_line:  Dictionary entry
    :returns a mapping between dictionary entry fields and values
    """
    #dict_line_patt = "([^\s]+)\s+([^\s]+)\s+\[([^]]*)\]\s+({([^}]+)})?\s*(/(.*)/)?\s*(#\s+(.*$))?"

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
# Globals/Initialisation
###############################################################################
sqlcon = sqlite3.connect(":memory:")
sqlcon.row_factory = sqlite3.Row    # Allow use of named columns in query results
sqlcon.create_function("REGEXP", 2, regexp)
sqlcur = sqlcon.cursor()
create_dict()
###############################################################################


###############################################################################
def main():
    """
    """
    jyut_search_term = DictSearchTerm("jyun.", DE_JYUTPING, True)
    eng_search_term = DictSearchTerm("surname", DE_ENGLISH, True)
    show_search([jyut_search_term, eng_search_term])
    show_single_search("阮", DE_TRAD, False)

###############################################################################

if __name__ == "__main__":
    main()
