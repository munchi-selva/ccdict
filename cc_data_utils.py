#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Utilities for parsing CC-CEDICT/CC-Canto dictionary data.

CC-CEDICT offers public domain Mandarin Chinese-English dictionary data:
https://cc-cedict.org/wiki/start.
CC-Canto (see https://cantonese.org/about.html) builds on CC-CEDICT by:
    1. Providing Jyutping renderings of CC-CEDICT entries whose definitions are
       also valid in Cantonese (CC-CEDICT-Canto below)
    2. Providing a separate listing of specifically Cantonese terms
"""

import itertools
import re
from typing import Optional

from canto_dict_types import DictField

#
# Next steps:
# 1. Write unit tests for ccdict.db creation
# 2. Change the return type of the parsing functions
# 3. Retest
#

#
# CC* dictionary file formats
#
# CC-CEDICT entry format (https://CC-CEDICT.org/wiki/format:syntax)
#   TRAD_CHIN SIMP_CHIN [PINYIN] /ENG 1/ENG 2/.../ENG N/
#
# CC-Canto entry format: augments the CC-CEDICT format with a Jyutping field
#   TRAD_CHIN SIMP_CHIN [PINYIN] {JYUTPING} /ENG 1/ENG 2/.../ENG N/
#
# CC-CEDICT-Canto format: effectively maps Pinyin to Jyutping
#   TRAD_CHIN SIMP_CHIN [PINYIN] {JYUTPING}
#
# Content follow # should be treated as comments
#
# From the above, each CC-CEDICT and CC-Canto entry maps a single Chinese term
# to one or more English translations.
# The following regular expressions allow CC-* lines to be parsed to a format
# that maps a single Chinese term to a single English translation, i.e.:
#   TRAD_CHIN SIMP_CHIN PINYIN JYUTPING ENG COMMENT
#
TRAD_PATT       = fr"(?P<{DictField.DF_TRAD}>[^\s]+)"
SIMP_PATT       = fr"(?P<{DictField.DF_SIMP}>[^\s]+)"
PINYIN_PATT     = fr"\[(?P<{DictField.DF_PINYIN}>[^]]*)\]"
JYUTPING_PATT   = fr"({{(?P<{DictField.DF_JYUTPING}>[^}}]+)}})"
ENG_PATT        = fr"(/(?P<{DictField.DF_ENGLISH}>.*)/)"
COMMENT_PATT    = fr"(#\s+(?P<{DictField.DF_COMMENT}>.*$))"
DICT_PATT       = fr"{TRAD_PATT}\s+{SIMP_PATT}\s+{PINYIN_PATT}\s+{JYUTPING_PATT}?\s*{ENG_PATT}?\s*{COMMENT_PATT}?"

#
# Target format for parsing of CC-* dictionary entries, maps a single Cantonese
# term to a single English translation
#
#                      TRAD SIMP PIN            JYUT           ENG            COMMENT
CantoDictEntry = tuple[str, str, Optional[str], Optional[str], Optional[str], str]


def is_comment(dict_line: str) -> re.Match[str] | None:
    """
    Returns True if the dictionary file line is a comment.

    Args:
        dict_line:  Dictionary file line

    Returns:
        True if dict_line is a comment
    """
    return re.match("#", dict_line)


def parse_dict_line(dict_line: str) -> list[CantoDictEntry] | None:
    """
    Parses a CC-* dictionary file line into Cantonese dictionary entries.

    Args:
        param  dict_line:  Dictionary file line that may be a dictionary entry

    Returns:
        A list of Cantonese-English translations extracted from the input line
    """
    m = re.match(DICT_PATT, dict_line)
    if m:
        groups = m.groupdict()
        jyut_transcriptions = [jt.strip().lower() for jt in groups[DictField.DF_JYUTPING].split("/") if jt.strip()] if groups[DictField.DF_JYUTPING] else [None]
        eng_defs = [ed.strip() for ed in groups[DictField.DF_ENGLISH].split("/") if ed.strip()]  if groups[DictField.DF_ENGLISH] else [None]
        # What effect would the following have on the SQL inserts?
        # eng_defs = groups[DE_FLD_ENGLISH].split("/") if groups[DE_FLD_ENGLISH] else [""]

        return [(groups[DictField.DF_TRAD], groups[DictField.DF_SIMP],
                 groups[DictField.DF_PINYIN].lower() if groups[DictField.DF_PINYIN] else None,
                 jyut_transcription, english_def, groups[DictField.DF_COMMENT])
                for jyut_transcription, english_def in itertools.product(jyut_transcriptions, eng_defs)]
    return None
###############################################################################


def parse_dict_file(dict_filename: str, max_entries: int = -1) -> list[CantoDictEntry]:
    """
    Parses a file of CC-* dictionary file into Cantonese dictionary entries.

    Args:
        dict_filename:  Name of the dictionary file
        max_entries:    Maximum number of entries to parse, -1 => parse the
                        entire file

    Returns:
        A list of Cantonese-English translations extracted from the input file
    """
    entries: list[CantoDictEntry] = []
    entries_processed = 0
    with open(dict_filename) as dict_file:
        for dict_line in dict_file:
            if max_entries > 0 and entries_processed >= max_entries:
                break

            if not is_comment(dict_line):
                entries.extend(parse_dict_line(dict_line))
                entries_processed += 1
    return entries
###############################################################################

