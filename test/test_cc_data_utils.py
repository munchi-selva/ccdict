import logging
import pytest
import sys

logging.info(f"Here we are, sys.path = {sys.path}!")

from ccdict.cc_data_utils import parse_dict_line, parse_dict_file
from conftest import ExpectedDictFileParseOutput, ExpectedParseOutput

def check_parse_result(expected_output: ExpectedParseOutput):
    """Helper method that checks if a CC-* line is correctly parsed."""
    for dict_line, expected_dict_entries in expected_output.items():
        assert parse_dict_line(dict_line) == expected_dict_entries

#
# Bash script for estimating the number of Canto dict entries in a CC-* file
#
# To ensure entry_count is updated by the loop, iterate over dictionary lines
# using "< < ()" redirection.
# See: https://www.baeldung.com/linux/while-loop-variable-scope
#
# ccdict_file=/mnt/d/src/cccanto/cccanto-webdist.txt
# entry_count=0
# while read dict_line
# do
#    entry_count=$(($entry_count + $(echo $dict_line | grep -o "/" | wc -l) -1))
# done < <(grep "^[^#]" $ccdict_file)
# echo $entry_count

def test_ce_lines_parse(cedict_parse_outputs: list[ExpectedParseOutput]) -> None:
    """Tests parsing of lines extracted from a CC-CEDICT file."""
    for expected_output in cedict_parse_outputs:
        check_parse_result(expected_output)

def test_ccanto_lines_parse(ccanto_parse_outputs: list[ExpectedParseOutput]) -> None:
    """Tests parsing of lines extracted from a CC-Canto file."""
    for expected_output in ccanto_parse_outputs:
        check_parse_result(expected_output)

def test_cedict_ccanto_lines_parse(cedict_ccanto_parse_outputs: list[ExpectedParseOutput]) -> None:
    """Tests parsing of lines extracted from a CC-CEDICT-Canto file."""
    for expected_output in cedict_ccanto_parse_outputs:
        check_parse_result(expected_output)

def test_ccanto_parse(cc_file_parse_outputs: ExpectedDictFileParseOutput) -> None:
    """Tests parsing of a CC-* files."""
    for dict_filename, expected_entry_count in cc_file_parse_outputs.items():
        parsed_entries = parse_dict_file(dict_filename)
        assert len(parsed_entries) == expected_entry_count
