#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pytest
from pprint import pformat

from ccdict.canto_dict_types import DictField
from ccdict.ccdict import CantoDict, DictSearchTerm
from conftest import ExpectedSearchResult

def check_search_result(canto_dict: CantoDict,
                        expected_search_result: ExpectedSearchResult):
    """Helper method that verifies a CantoDict search result."""
    for search_term, search_result in expected_search_result.items():
        assert canto_dict.search_dict(search_term) == search_result

def test_search_dict(canto_dict: CantoDict,
                     canto_dict_search_results: list[ExpectedSearchResult]) -> None:

    for expected_result in canto_dict_search_results:
        check_search_result(canto_dict, expected_result)

#   logging.info(f'Traditional Chinese string search result =\n{pformat(canto_dict.search_dict("謙"))}')

#   logging.info(f'Traditional Chinese string search result =\n{pformat(canto_dict.search_dict("餅"))}')

#   jyutping_search_term = DictSearchTerm(search_value="beng2", search_field=DictField.DF_JYUTPING, use_re=False)
#   logging.info(f'Jyutping term search_result =\n{pformat(canto_dict.search_dict([jyutping_search_term]))}')
