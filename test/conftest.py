#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pytest
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Union

from ccdict.canto_dict_types import DictField
from ccdict.cc_data_utils import CantoDictEntry
from ccdict.ccdict import CantoDict, DICT_DB_FILENAME, DictSearchTerm

# Maps a dictionary line to its expected parsed Canto dictionary entries
type ExpectedParseOutput = dict[str, list[CantoDictEntry]]

type ExpectedDictFileParseOutput = dict[str, int]

@dataclass
class DictSearchTestCase:
    """Defines a CantoDict search test case as a search expression + result."""
    search_expr: Union[str, list[DictSearchTerm]]
    search_result: list[dict]

@pytest.fixture(scope="session")
def canto_dict(tmp_path_factory: pytest.TempPathFactory) -> CantoDict:
    dict_data_path = tmp_path_factory.mktemp("dict_data")
    #yield CantoDict(dict_db_filename = str(dict_data_path / "ccdict.db"))
    yield CantoDict(dict_db_filename = DICT_DB_FILENAME)

@pytest.fixture
def cedict_symbols_in_chinese() -> ExpectedParseOutput:
    yield {
        "% % [pa1] /percent (Tw)/": [("%", "%", "pa1", None, "percent (Tw)", None)],
    }

@pytest.fixture
def cedict_symbols_in_english() -> ExpectedParseOutput:
    yield {
        "A圈兒 A圈儿 [A quan1 r5] /at symbol, @/":
            [("A圈兒", "A圈儿", "a quan1 r5", None, "at symbol, @", None)],
        "A菜 A菜 [A cai4] /(Tw) A-choy, or Taiwanese lettuce (Lactuca sativa var. sativa) (from Taiwanese 萵仔菜, Tai-lo pr. [ue-á-tshài] or [e-á-tshài])/":
            [("A菜", "A菜", "a cai4", None, "(Tw) A-choy, or Taiwanese lettuce (Lactuca sativa var. sativa) (from Taiwanese 萵仔菜, Tai-lo pr. [ue-á-tshài] or [e-á-tshài])", None)],
        "VCR VCR [V C R] /video clip (loanword from \"videocassette recorder\")/":
            [
                ("VCR", "VCR", "v c r", None, "video clip (loanword from \"videocassette recorder\")", None)
            ],
        "井號 井号 [jing3 hao4] /number sign # (punctuation)/hash symbol/pound sign/":
            [
                ("井號", "井号", "jing3 hao4", None, "number sign # (punctuation)", None),
                ("井號", "井号", "jing3 hao4", None, "hash symbol", None),
                ("井號", "井号", "jing3 hao4", None, "pound sign", None)
            ],
        "素質差 素质差 [su4 zhi4 cha4] /so uneducated!/so ignorant!/":
            [
                ("素質差", "素质差", "su4 zhi4 cha4", None, "so uneducated!", None),
                ("素質差", "素质差", "su4 zhi4 cha4", None, "so ignorant!", None)
            ],
        "索邦大學 索邦大学 [Suo3 bang1 Da4 xue2] /Université Paris IV/the Sorbonne/":
            [
                ("索邦大學", "索邦大学", "suo3 bang1 da4 xue2", None, "Université Paris IV", None),
                ("索邦大學", "索邦大学", "suo3 bang1 da4 xue2", None, "the Sorbonne", None)
            ]
    }

@pytest.fixture
def cedict_names() -> ExpectedParseOutput:
    yield {
        "朱 朱 [Zhu1] /surname Zhu/":
            [("朱", "朱", "zhu1", None, "surname Zhu", None)],
        "朱 朱 [zhu1] /vermilion/":
            [("朱", "朱", "zhu1", None, "vermilion", None)],
        "朱麗葉 朱丽叶 [Zhu1 li4 ye4] /Juliet or Juliette (name)/":
            [("朱麗葉", "朱丽叶", "zhu1 li4 ye4", None, "Juliet or Juliette (name)", None)],
        "葉 叶 [Ye4] /surname Ye/":
            [("葉", "叶", "ye4", None, "surname Ye", None)],
        "葉 叶 [ye4] /leaf/page/lobe/(historical) period/classifier for small boats/":
            [
                ("葉", "叶", "ye4", None, "leaf", None),
                ("葉", "叶", "ye4", None, "page", None),
                ("葉", "叶", "ye4", None, "lobe", None),
                ("葉", "叶", "ye4", None, "(historical) period", None),
                ("葉", "叶", "ye4", None, "classifier for small boats", None)
             ]
    }

def cedict_multiple_glosses() -> ExpectedParseOutput:
    yield {
        "十六字訣 十六字诀 [shi2 liu4 zi4 jue2] /16-character formula, esp. Mao Zedong's mantra on guerrilla warfare: 敵進我退，敵駐我擾，敵疲我打，敵退我追|敌进我退，敌驻我扰，敌疲我打，敌退我追[di2 jin4 wo3 tui4 , di2 zhu4 wo3 rao3 , di2 pi2 wo3 da3 , di2 tui4 wo3 zhui1] when the enemy advances we retreat; when the enemy makes camp we harass; when the enemy is exhausted we fight; and when the enemy retreats we pursue/":
        [("十六字訣", "十六字诀", "shi2 liu4 zi4 jue2", None, "16-character formula, esp. Mao Zedong's mantra on guerrilla warfare: 敵進我退，敵駐我擾，敵疲我打，敵退我追|敌进我退，敌驻我扰，敌疲我打，敌退我追[di2 jin4 wo3 tui4 , di2 zhu4 wo3 rao3 , di2 pi2 wo3 da3 , di2 tui4 wo3 zhui1] when the enemy advances we retreat; when the enemy makes camp we harass; when the enemy is exhausted we fight; and when the enemy retreats we pursue", None)]
    }

@pytest.fixture
def ccanto_parse_idioms() -> ExpectedParseOutput:
    yield {
        "㷫過焫雞 㷫过焫鸡 [qing3 guo4 ruo4 ji1] {hing3 gwo3 naat3 gai1} /extremely angry/":
            [("㷫過焫雞", "㷫过焫鸡", "qing3 guo4 ruo4 ji1",  "hing3 gwo3 naat3 gai1",  "extremely angry", None)],
        "先禮後兵 先礼后兵 [xian1 li3 hou4 bing1] {sin1 lai5 hau6 bing1} /(idiom) (figurative) Negotiation will be used to reach a solution before using military force/":
            [("先禮後兵", "先礼后兵", "xian1 li3 hou4 bing1", "sin1 lai5 hau6 bing1", "(idiom) (figurative) Negotiation will be used to reach a solution before using military force", None)]
    }

@pytest.fixture
def ccanto_parse_cantoisms() -> ExpectedParseOutput:
    yield {
        "啋 啋 [cai3] {coi1} /(Cant.) an interjection used to berate someone/an expression said in an attempt to 'counteract' or 'cancel out' the effect when someone else has said something unlucky or unpleasant/ # adapted from cc-cedict":
            [
                ("啋", "啋", "cai3", "coi1", "(Cant.) an interjection used to berate someone", "adapted from cc-cedict"),
                ("啋", "啋", "cai3", "coi1",
                 "an expression said in an attempt to 'counteract' or 'cancel out' the effect when someone else has said something unlucky or unpleasant", "adapted from cc-cedict")
            ],
        "啹 啹 [ju2] {geoi1} /(Cant.)/stupid/idiotic/to kill/to slaughter/to roll or crumple into a ball/phonetic, such as in 'gurkha'/ # adapted from cc-cedict":
            [
                ("啹", "啹", "ju2", "geoi1", "(Cant.)", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "geoi1", "stupid", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "geoi1", "idiotic", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "geoi1", "to kill", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "geoi1", "to slaughter", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "geoi1", "to roll or crumple into a ball", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "geoi1", "phonetic, such as in 'gurkha'", "adapted from cc-cedict")
             ],
        "啹 啹 [ju2] {goei1} /(Cant.)/stupid/idiotic/to kill/to slaughter/to roll or crumple into a ball/phonetic, such as in 'gurkha'/ # adapted from cc-cedict":
            [
                ("啹", "啹", "ju2", "goei1", "(Cant.)", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goei1", "stupid", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goei1", "idiotic", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goei1", "to kill", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goei1", "to slaughter", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goei1", "to roll or crumple into a ball", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goei1", "phonetic, such as in 'gurkha'", "adapted from cc-cedict")
             ],
        "啹 啹 [ju2] {koe1} /(Cant.)/stupid/idiotic/to kill/to slaughter/to roll or crumple into a ball/phonetic, such as in 'gurkha'/ # adapted from cc-cedict":
            [
                ("啹", "啹", "ju2", "koe1", "(Cant.)", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "koe1", "stupid", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "koe1", "idiotic", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "koe1", "to kill", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "koe1", "to slaughter", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "koe1", "to roll or crumple into a ball", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "koe1", "phonetic, such as in 'gurkha'", "adapted from cc-cedict")
             ],
        "啹 啹 [ju2] {goe4} /to accept/to not object/to feel better/to burp/ # adapted from cc-cedict":
            [
                ("啹", "啹", "ju2", "goe4", "to accept", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goe4", "to not object", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goe4", "to feel better", "adapted from cc-cedict"),
                ("啹", "啹", "ju2", "goe4", "to burp", "adapted from cc-cedict"),
            ]
    }

@pytest.fixture
def cedict_ccanto_sampling() -> ExpectedParseOutput:
    yield {
        "伊莉莎白 伊莉莎白 [Yi1 li4 sha1 bai2] {ji1 lei6 saa1 baak6}":
            [
                ("伊莉莎白", "伊莉莎白", "yi1 li4 sha1 bai2", "ji1 lei6 saa1 baak6", None, None)
            ]
    }

@pytest.fixture
def cedict_ccanto_parse_outputs(
    cedict_ccanto_sampling: ExpectedParseOutput
) -> list[ExpectedParseOutput]:
    yield [cedict_ccanto_sampling]

@pytest.fixture
def cedict_parse_outputs(
    cedict_symbols_in_chinese: ExpectedParseOutput,
    cedict_symbols_in_english: ExpectedParseOutput,
    cedict_names: ExpectedParseOutput
) -> list[ExpectedParseOutput]:
    yield [cedict_symbols_in_chinese, cedict_symbols_in_english, cedict_names]

@pytest.fixture
def ccanto_parse_outputs(
    ccanto_parse_idioms: ExpectedParseOutput,
    ccanto_parse_cantoisms: ExpectedParseOutput
) -> list[ExpectedParseOutput]:
    yield [ccanto_parse_idioms, ccanto_parse_cantoisms]

@pytest.fixture
def cc_filenames() -> list[str]:
    yield [
        "/mnt/d/src/cccanto/cccanto-webdist.txt",
        "/mnt/d/src/cccanto/cedict_1_0_ts_utf-8_mdbg.txt",
        "/mnt/d/src/cccanto/cccedict-canto-readings-150923.txt"
    ]

@pytest.fixture
def cc_file_parse_outputs(cc_filenames: list[str]) -> ExpectedDictFileParseOutput:
    """Yields the expected output of parsing CC-* dictionary files.

    Makes use of an awk script to count the expected number of dictionary
    entries in each file.
    """
    count_script_path = Path(__file__).parent / "count_dict_entries.awk"
    cc_file_parse_outputs = {}

    for cc_filename in cc_filenames:
        count_script_output = subprocess.run(
            ["awk", "-f", str(count_script_path), cc_filename],
            capture_output=True,
            encoding="utf-8")
        if not count_script_output.returncode:
            cc_file_parse_outputs[cc_filename] = int(count_script_output.stdout[:-1])

    return cc_file_parse_outputs

@pytest.fixture
def multi_cj_search_test_cases() -> list[DictSearchTestCase]:
    """Yields search test cases for terms with multiple CJ codes."""
    yield [
            DictSearchTestCase(
                "艦",
                 [
                     {
                         DictField.DF_TRAD.value: "艦",
                         DictField.DF_SIMP.value: "舰",
                         DictField.DF_PINYIN.value: "jian4",
                         DictField.DF_JYUTPING.value: ["laam6"],
                         DictField.DF_ENGLISH.value: ["naval vessel", "warship"],
                         DictField.DF_CJCODE.value: ["hysit", "hysmt"]
                     }
                 ]
            )
        ]

@pytest.fixture
def multi_jyutping_search_test_cases() -> list[DictSearchTestCase]:
    """Yields search test cases for terms with multiple Jyutping transcriptions."""
    yield [
        DictSearchTestCase(
            "吼",
            [
                {
                    DictField.DF_TRAD.value: "吼",
                    DictField.DF_SIMP.value: "吼",
                    DictField.DF_PINYIN.value: "hou3",
                    DictField.DF_JYUTPING.value: ["haau1", "hau2", "hau3"],
                    DictField.DF_ENGLISH.value: ["bellow of rage", "roar or howl of an animal"],
                    DictField.DF_CJCODE.value: ["rndu"]
                },
                {
                    DictField.DF_TRAD.value: "吼",
                    DictField.DF_SIMP.value: "吼",
                    DictField.DF_PINYIN.value: "hou3",
                    DictField.DF_JYUTPING.value: ["haau1"],
                    DictField.DF_ENGLISH.value: ["(verb) watch closely on something because of great interest",
                                                 "to howl", "to roar", "to shriek"],
                    DictField.DF_CJCODE.value: ["rndu"]
                },
                {
                    DictField.DF_TRAD.value: "吼",
                    DictField.DF_SIMP.value: "吼",
                    DictField.DF_PINYIN.value: "hou3",
                    DictField.DF_JYUTPING.value: ["hau1", "hau4"],
                    DictField.DF_ENGLISH.value: ["to keep an eye on", "to long for",
                                                 "to lust after", "to target",
                                                 "to watch closely"],
                    DictField.DF_CJCODE.value: ["rndu"]
                }
            ]
        ),
        DictSearchTestCase(
            "樂",
            [
                {
                    DictField.DF_TRAD.value: "樂",
                    DictField.DF_SIMP.value: "乐",
                    DictField.DF_PINYIN.value: "le4",
                    DictField.DF_JYUTPING.value: ["lok3"],
                    DictField.DF_ENGLISH.value: ["a place name", "a surname"],
                    DictField.DF_CJCODE.value: ["vid"]
                },
                {
                    DictField.DF_TRAD.value: "樂",
                    DictField.DF_SIMP.value: "乐",
                    DictField.DF_JYUTPING.value: ["lok6"],
                    DictField.DF_PINYIN.value: "le4,yue4",
                    DictField.DF_ENGLISH.value: ["cheerful",
                                                 "enjoyable",
                                                 "happy",
                                                 "musical; music",
                                                 "surname Le",
                                                 "surname Yue",
                                                 "to laugh"],
                   DictField.DF_CJCODE.value: ["vid"]
                },
                {
                    DictField.DF_TRAD.value: "樂",
                    DictField.DF_SIMP.value: "乐",
                    DictField.DF_PINYIN.value: "le4",
                    DictField.DF_JYUTPING.value: ["ngaau6"],
                    DictField.DF_ENGLISH.value: ["to love, to be fond of, to delight in"],
                    DictField.DF_CJCODE.value: ["vid"]
                },
                {
                    DictField.DF_TRAD.value: "樂",
                    DictField.DF_SIMP.value: "乐",
                    DictField.DF_PINYIN.value: "le4,yue4",
                    DictField.DF_JYUTPING.value: ["ngok6", "lok6"],
                    DictField.DF_ENGLISH.value: ["music"],
                    DictField.DF_CJCODE.value: ["vid"]
                }
            ]
        )
    ]

@pytest.fixture
def composite_search_test_cases() -> list[DictSearchTestCase]:
    """Yields search test cases with multiple search terms."""
    yield [
        DictSearchTestCase(
            [
                DictSearchTerm("jyun.", DictField.DF_JYUTPING, True), DictSearchTerm("surname", DictField.DF_ENGLISH, True)
            ],
            [
                {
                    DictField.DF_TRAD: "元",
                    DictField.DF_SIMP: "元",
                    DictField.DF_PINYIN: "yuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["a surname", "surname Yuan"],
                    DictField.DF_CJCODE: ["mmu"]
                },
                {
                    DictField.DF_TRAD: "原",
                    DictField.DF_SIMP: "原",
                    DictField.DF_PINYIN: "yuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["(Japanese surname) Hara", "surname Yuan"],
                    DictField.DF_CJCODE: ["mhaf"]
                },
                {
                    DictField.DF_TRAD: "園",
                    DictField.DF_SIMP: "园",
                    DictField.DF_PINYIN: "yuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["surname Yuan"],
                    DictField.DF_CJCODE: ["wgrv"]
                },
                {
                    DictField.DF_TRAD: "完",
                    DictField.DF_SIMP: "完",
                    DictField.DF_PINYIN: "wan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["a surname"],
                    DictField.DF_CJCODE: ["jmmu"]
                },
                {
                    DictField.DF_TRAD: "小淵",
                    DictField.DF_SIMP: "小渊",
                    DictField.DF_PINYIN: "xiao3 yuan1",
                    DictField.DF_JYUTPING: ["siu2 jyun1"],
                    DictField.DF_ENGLISH: ["Obuchi (Japanese surname)"],
                    DictField.DF_CJCODE: []
                },
                {
                    DictField.DF_TRAD: "淵",
                    DictField.DF_SIMP: "渊",
                    DictField.DF_PINYIN: "yuan1",
                    DictField.DF_JYUTPING: ["jyun1"],
                    DictField.DF_ENGLISH: ["a surname"],
                    DictField.DF_CJCODE: ["elxl"]
                },
                {
                    DictField.DF_TRAD: "源",
                    DictField.DF_SIMP: "源",
                    DictField.DF_PINYIN: "yuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["surname"],
                    DictField.DF_CJCODE: ["emhf"]
                },
                {
                    DictField.DF_TRAD: "爰",
                    DictField.DF_SIMP: "爰",
                    DictField.DF_PINYIN: "yuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["and then a surname"],
                    DictField.DF_CJCODE: ["bmke"]
                },
                {
                    DictField.DF_TRAD: "玄",
                    DictField.DF_SIMP: "玄",
                    DictField.DF_PINYIN: "xuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["a surname"],
                    DictField.DF_CJCODE: ["yvi"]
                },
                {
                    DictField.DF_TRAD: "苑",
                    DictField.DF_SIMP: "苑",
                    DictField.DF_PINYIN: "yuan4",
                    DictField.DF_JYUTPING: ["jyun2"],
                    DictField.DF_ENGLISH: ["a surname", "surname Yuan"],
                    DictField.DF_CJCODE: ["tniu"]
                },
                {
                    DictField.DF_TRAD: "蜎",
                    DictField.DF_SIMP: "蜎",
                    DictField.DF_PINYIN: "yuan1",
                    DictField.DF_JYUTPING: ["jyun1"],
                    DictField.DF_ENGLISH: ["surname Yuan"],
                    DictField.DF_CJCODE: ["lirb"]
                },
                {
                    DictField.DF_TRAD: "袁",
                    DictField.DF_SIMP: "袁",
                    DictField.DF_PINYIN: "yuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["a surname"],
                    DictField.DF_CJCODE: ["grhv"]
                },
                {
                    DictField.DF_TRAD: "軒轅",
                    DictField.DF_SIMP: "轩辕",
                    DictField.DF_PINYIN: "xuan1 yuan2",
                    DictField.DF_JYUTPING: ["hin1 jyun4"],
                    DictField.DF_ENGLISH: ["two-character surname Xuanyuan"],
                    DictField.DF_CJCODE: []
                },
                {
                    DictField.DF_TRAD: "轅",
                    DictField.DF_SIMP: "辕",
                    DictField.DF_PINYIN: "yuan2",
                    DictField.DF_JYUTPING: ["jyun4"],
                    DictField.DF_ENGLISH: ["a surname"],
                    DictField.DF_CJCODE: ["jjgrv"]
                },
                {
                    DictField.DF_TRAD: "阮",
                    DictField.DF_SIMP: "阮",
                    DictField.DF_PINYIN: "ruan3",
                    DictField.DF_JYUTPING: ["jyun2", "jyun5"],
                    DictField.DF_ENGLISH: ["surname Ruan"],
                    DictField.DF_CJCODE: ["nlmmu"]
                }
            ]
        )
    ]

@pytest.fixture
def canto_dict_search_test_cases(
    multi_cj_search_test_cases: list[DictSearchTestCase],
    multi_jyutping_search_test_cases: list[DictSearchTestCase],
    composite_search_test_cases: list[DictSearchTestCase]
) -> list[DictSearchTestCase]:
    yield (
        multi_cj_search_test_cases +
        multi_jyutping_search_test_cases +
        composite_search_test_cases
    )
