#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Types!"""

from enum import StrEnum

class DictField(StrEnum):
    """Dictionary entry field names, used as SQL table column names, etc."""
    DF_TRAD     = "traditional"
    DF_SIMP     = "simplified"
    DF_PINYIN   = "pinyin"
    DF_JYUTPING = "jyutping"
    DF_ENGLISH  = "english"
    DF_COMMENT  = "comment"
    DF_CJCODE   = "cjcode"
    DF_CJCHAR   = "character"

DICT_FIELD_NAMES = [dict_field.name for dict_field in DictField]
DICT_FIELDS = [dict_field.value for dict_field in DictField]
