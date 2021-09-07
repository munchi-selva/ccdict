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
import re
import sqlite3

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
    return re_pattern.match(field) is not None
###############################################################################


###############################################################################
def create_dict():
    """
    Create and populates the dictionary table(s)
    """
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

    canto_tuples = parse_dict_entries("{}/{}".format(CC_DIR, CCCANTO_FILE))
    sqlcon.executemany("INSERT INTO cc_canto VALUES(?, ?, ?, ?, ?, ?)", canto_tuples)

    cedict_tuples = parse_dict_entries("{}/{}".format(CC_DIR, CCCEDICT_FILE))
    sqlcon.executemany("INSERT INTO cc_cedict VALUES(?, ?, ?, ?, ?, ?)",
            cedict_tuples)

    cedict_canto_tuples = parse_dict_entries("{}/{}".format(CC_DIR, CCCEDICT_CANTO_FILE))
    sqlcon.executemany("INSERT INTO cc_cedict_canto VALUES(?, ?, ?, ?, ?, ?)",
        cedict_canto_tuples)

    cedict_join_query = "INSERT INTO cc_canto \
                         SELECT c.traditional, c.simplified, \
                                c.pinyin, cc.jyutping, c.english, c.comment \
                         FROM   cc_cedict c JOIN cc_cedict_canto cc \
                                ON  c.traditional = cc.traditional AND \
                                    c.simplified = cc.simplified AND \
                                    c.pinyin = cc.pinyin"
    sqlcon.execute(cedict_join_query)

    cedict_orphans_query = "INSERT INTO cc_canto \
                            SELECT c.traditional, c.simplified, \
                                   c.pinyin, cc.jyutping, c.english, c.comment \
                            FROM   cc_cedict c LEFT JOIN cc_cedict_canto cc \
                            ON     c.traditional = cc.traditional AND \
                                   c.simplified = cc.simplified AND \
                                   c.pinyin = cc.pinyin \
                            WHERE  cc.jyutping IS NULL"
    sqlcon.execute(cedict_orphans_query)

    cedict_canto_orphans_query = "INSERT INTO cc_canto \
                                  SELECT cc.traditional, cc.simplified, \
                                         cc.pinyin, cc.jyutping, cc.english, cc.comment \
                                  FROM   cc_cedict_canto cc LEFT JOIN cc_cedict c \
                                  ON     c.traditional = cc.traditional AND \
                                         c.simplified = cc.simplified AND \
                                         c.pinyin = cc.pinyin \
                                  WHERE  c.traditional IS NULL"
    sqlcon.execute(cedict_canto_orphans_query)
###############################################################################


###############################################################################
def lookup_dict(search_term):
    # type (str) -> None
    """
    Looks up a term in the dictionary
    """

    #
    # Build a query that aggregates definitions for the search term
    # corresponding to the same Jyutping value
    #
    canto_query = "SELECT traditional AS traditional, jyutping AS jyutping, pinyin AS pinyin, group_concat(english, '/') AS defns\
                   FROM cc_canto \
                   WHERE traditional REGEXP ? \
                   GROUP BY jyutping, pinyin"
    for row in sqlcon.execute(canto_query, (search_term,)):
        defns = row["defns"]
        if defns:
            defns = list(set(row["defns"].split("/")))
            defns.sort()
        print("{}\n\t{} ({})".format(row["traditional"], row["jyutping"], row["pinyin"]))
        if defns:
            for defn in defns:
                print("\t{}".format(defn))
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
                cccanto_tuple = parse_dict_line(dict_line)
                entries.append(cccanto_tuple)
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
        return (groups[DE_TRAD], groups[DE_SIMP], groups[DE_PINYIN], groups[DE_JYUTPING], groups[DE_ENGLISH], groups[DE_COMMENT])
        #return m.groups()

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
# Globals
###############################################################################
sqlcon = sqlite3.connect(":memory:")
sqlcon.row_factory = sqlite3.Row    # Allow use of named columns in query results
sqlcon.create_function("REGEXP", 2, regexp)


###############################################################################
def main():
    """
    """

###############################################################################

if __name__ == "__main__":
    main()
    create_dict()
    lookup_dict("五.*")
