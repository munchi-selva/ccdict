import logging
import pytest
import sys

logging.info(f"Here we are, sys.path = {sys.path}!")

from ccdict.cc_data_utils import parse_dict_line, parse_dict_file
from conftest import ExpectedParseOutput


def check_parse_result(expected_output: ExpectedParseOutput):
    """Helper method that checks if a line is correctly parsed."""
    for dict_line, expected_dict_entries in expected_output.items():
        assert parse_dict_line(dict_line) == expected_dict_entries

def test_ce_parse(cedict_parse_outputs: list[ExpectedParseOutput]) -> None:
    for expected_output in cedict_parse_outputs:
        check_parse_result(expected_output)

def test_ccanto_parse(ccanto_parse_outputs: list[ExpectedParseOutput]) -> None:
    for expected_output in ccanto_parse_outputs:
        check_parse_result(expected_output)

def test_cedict_ccanto_parse(cedict_ccanto_parse_outputs: list[ExpectedParseOutput]) -> None:
    for expected_output in cedict_ccanto_parse_outputs:
        check_parse_result(expected_output)

