#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pytest
from pprint import pformat

from ccdict.canto_dict_types import DictField
from ccdict.ccdict import CantoDict, DictSearchTerm
from conftest import DictSearchTestCase

def verify_search(
    canto_dict: CantoDict, dict_search_test_case: DictSearchTestCase
) -> None:
    """Helper method that verifies a CantoDict search case."""
    assert canto_dict.search_dict(dict_search_test_case.search_expr,
                                  **dict_search_test_case.search_options) == dict_search_test_case.search_result

def test_search_dict(
    canto_dict: CantoDict,
    canto_dict_search_test_cases: list[DictSearchTestCase]
) -> None:
    for search_test_case in canto_dict_search_test_cases:
        verify_search(canto_dict, search_test_case)
