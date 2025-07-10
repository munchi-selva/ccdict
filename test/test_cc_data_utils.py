import pytest
import sys
from collections import defaultdict

from ccdict.cc_data_utils import parse_dict_line, parse_dict_file
from conftest import ExpectedDictFileParseOutput, ExpectedParseOutput

def check_parse_result(expected_output: ExpectedParseOutput):
    """Helper method that checks if a CC-* line is correctly parsed."""
    for dict_line, expected_dict_entries in expected_output.items():
        assert parse_dict_line(dict_line) == expected_dict_entries

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

#       parsed_mapping: defaultdict[str, int] = defaultdict(int)
#       for entry in parsed_entries:
#           parsed_mapping[entry[0]] += 1

#       with open(f"/tmp/parse_result", "w") as parse_result_file:
#           for k, v in parsed_mapping.items():
#               parse_result_file.write(f"{k}\t{v}\n")

        assert len(parsed_entries) == expected_entry_count
