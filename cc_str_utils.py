#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""String handling utilities for working with canto dictionary data."""

# List of Unicode ranges that include Han ideographs
HAN_UNICODE_RANGES: list[range] = [
    range(0x4E00, 0x9FFF+1),     # CJK Unified Ideographs Common
    range(0x3400, 0x4DBF+1),     # CJK Unified Ideographs Extension A Rare
    range(0x20000, 0x2A6DF+1),   # CJK Unified Ideographs Extension B Rare, historic
    range(0x2A700, 0x2B73F+1),   # CJK Unified Ideographs Extension C Rare, historic
    range(0x2B740, 0x2B81F+1),   # CJK Unified Ideographs Extension D Uncommon, some in current use
    range(0x2B820, 0x2CEAF +1),  # CJK Unified Ideographs Extension E Rare, historic
    range(0x2CEB0, 0x2EBEF +1),  # CJK Unified Ideographs Extension F Rare, historic
    range(0x30000, 0x3134F +1),  # CJK Unified Ideographs Extension G Rare, historic
    range(0x31350, 0x323AF +1),  # CJK Unified Ideographs Extension H Rare, historic
    range(0xF900, 0xFAFF +1),    # CJK Compatibility Ideographs Duplicates, unifiable variants, corporate characters
    range(0x2F800, 0x2FA1F +1)   # CJK Compatibility Ideographs Supplement Unifiable variants
]

def contains_han(string: str) -> bool:
    """Returns true if a string contains Han ideographs."""
    return any(ord(letter) in han_range for han_range in HAN_UNICODE_RANGES for letter in string)

