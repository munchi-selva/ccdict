#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""An interactive shell for searching a CantoDict dictionary."""


import click
import logging
import sys
from pprint import pformat
from enum import IntEnum

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypeVar, Union

from click_shell import make_click_shell, Shell
from shell_with_default.shell_with_default import click_group_with_default, ClickShellWithDefault

# Not sure yet if separate loggers should be used per module
canto_logger    = logging.getLogger(__name__)
console_handler = logging.StreamHandler(sys.stdout)
canto_logger.addHandler(console_handler)
canto_logger.setLevel(logging.INFO)

from ccdict import (
    CantoDict,
    DE_FLD_TRAD,
    DE_FLD_SIMP,
    DE_FLD_PINYIN,
    DE_FLD_JYUTPING,
    DE_FLD_ENGLISH,
    DE_FLD_CJCODE,
    DE_FLDS_NAMES,
    DICT_DB_FILENAME,
    parse_dict_search_cmd,
    SEARCH_CMD_TOKENS
)


# I think this was a pipedream for setting up default options
DictSearchOptId = IntEnum("DictSearchOptId",  "DSO_TRY_ALL \
                                               DSO_LAZY \
                                               DSO_FLATTEN_PINYIN \
                                               DSO_DO_RE_SEARCH \
                                               DSO_SEARCH_FLD \
                                               DSO_DISPLAY_FLDS \
                                               DSO_DISPLAY_FMT \
                                               DSO_DISPLAY_INDENT")

DictSearchOutputFormat = IntEnum("DictSearchOutputFormat",  "DSOF_ASCII \
                                                             DSOF_JSON")

@dataclass
class DictSearchOpt:
    T = TypeVar('T')

    id: DictSearchOptId
    type: TypeVar('T')
    default_value: type
    curr_val: Optional[type] = None


#@click_group_with_default(prompt="ccdict $ ", debug=False, custom_parser=parse_dict_search_cmd)
@click_group_with_default(prompt="ccdict $ ", debug=False)
@click.pass_context
def ccdict_shell(ctx: click.Context):
    ctx.allow_extra_args = True
    ctx.ignore_unknown_options = True
    # Ensure that ctx.obj exists and is a dict
    ctx.ensure_object(dict)

    # Use ctx.obj to store the dictionary
    ctx.obj["dictionary"] = CantoDict(DICT_DB_FILENAME)
#   initial_opts: List[DictSearchOpt] = [DictSearchOpt(id=DictSearchOptId.DSO_DISPLAY_FMT, type=DictSearchOutputFormat, default_value=DictSearchOutputFormat.DSOF_ASCII)]
#   ctx.obj["opts"]: Dict[DictSearchOptId, DictSearchOpt] = {initial_opt.id: initial_opt for initial_opt in initial_opts}


@ccdict_shell.command(default=True)
@click.pass_context
@click.argument("search_term", type=str)

# Search behaviour options
@click.option("--all/--not-all", default=True, help="Search for matches for the search term across different search fields")
@click.option("--lazy/--not-lazy", default=True, help="Stop searching as soon as matches are found on one search field")
@click.option("--re/--no-re", default=None, help="Use regular expression searches")
#@click.option("-r", "--use-re", is_flag=True, default=None)
                          #(DICT_OPT_SRCH_FLD,       "str",  None, True),
                          #(DICT_OPT_DISP_INDENT,    "str",  "", False)]
@click.option("--flatten/--not-flatten", default=True, help="Treat two definitions for a DE_FLD_TRAD value even if their DE_FLD_PINYIN values differ")

# Search result display options
@click.option("-d", "--display-field",
              type=click.Choice(DE_FLDS_NAMES), multiple=True, default=["DE_FLD_TRAD", "DE_FLD_CJCODE", "DE_FLD_JYUTPING", "DE_FLD_ENGLISH"],
              help="Include the specified field in the search output")
@click.option("-c", "--compact", is_flag=True, default=False, help="Compact the search result to a single line")
@click.option("-f", "--output-format", type=click.Choice(["ASCII", "JSON"]), default="ASCII", help="Format of the search result")
def search(ctx: click.Context, search_term: str,
           all: bool,
           lazy: bool,
           re: Optional[bool],
           flatten: bool,
           display_field: List[str],
           compact: bool,
           output_format: str) -> List[str]: #None:
    cmd_comps =  parse_dict_search_cmd(search_term,
                                       cmd_tkn_defs = SEARCH_CMD_TOKENS)
    cmd_comps["try_all_fields"] = all
    cmd_comps["lazy_eval"] = lazy
    cmd_comps["use_re"] = re
    cmd_comps["flatten_pinyin"] = flatten
    cmd_comps["fields"] = [eval(field_name) for field_name in display_field]
    cmd_comps["output_format"] = CantoDict.DictOutputFormat.__getitem__(f"DOF_{output_format}")
    cmd_comps["compact"] = compact

    canto_logger.debug(f"Search command components: {pformat(cmd_comps)}")
    search_expr = cmd_comps.pop("search_expr", None)
    if search_expr:
        return ctx.obj["dictionary"].show_search(search_expr, **(cmd_comps))
    return []


###############################################################################


###############################################################################
def main():
    """
    """
    # Test search terms
    multi_jyutping_for_same_definition = "吼"

#   canto_dict = CantoDict(DICT_DB_FILENAME)
#   canto_dict.show_search("艦")

#   pre_lookup_time = time.perf_counter()
#   dict_lookup_result = canto_dict.get_formatted_search_results(multi_jyutping_for_same_definition,
#                                                                search_field=DE_FLD_TRAD,
#                                                                output_format=CantoDict.DictOutputFormat.DOF_JSON, fields=["jyutping", "english"])
#   post_lookup_time = time.perf_counter()
#   print(dict_lookup_result)
#   print(f"Lookup time without indexes: {post_lookup_time-pre_lookup_time:0.4f} seconds")

#   canto_dict = CantoDict(DICT_DB_FILENAME, build_indexes=True)
#   pre_lookup_time = time.perf_counter()
#   dict_lookup_result = canto_dict.get_formatted_search_results(multi_jyutping_for_same_definition,
#                                                                search_field=DE_FLD_TRAD,
#                                                                output_format=CantoDict.DictOutputFormat.DOF_JSON, fields=["jyutping", "english"])
#   post_lookup_time = time.perf_counter()
#   print(dict_lookup_result)
#   print(f"Lookup time with indexes: {post_lookup_time-pre_lookup_time:0.4f} seconds")

#   jyut_search_term = DictSearchTerm("jyun.", DE_FLD_JYUTPING, True)
#   eng_search_term = DictSearchTerm("surname", DE_FLD_ENGLISH, True)
#   canto_dict.show_search([jyut_search_term, eng_search_term], indent_str = "\t")
#   canto_dict.show_search("樂", fields = DE_FLDS)
#   canto_dict.show_search("樂", fields = DE_FLDS, flatten_pinyin = False, indent_str = "!!!!")
#   canto_dict.show_search("艦")

#   t = DictSearchTermCmdTkn(CmdTknType.CTT_SEARCH_TERM, r"\(", r"\)", False)
#   test_strs = [
#                   'DE_FLD_ENGLISH "Hello" true',
#                   'DE_FLD_ENGLISH "Hello"',
#                   'DE_FLD_ENGLISH   hello  ',
#                   'DE_FLD_ENGLISH',
#                   'DE_FLD_TRAD   "^我.*$"   true'
#               ]
#   for tkn_src_str in test_strs:
#       cmd_content = t.get_cmd_content(tkn_src_str, 0, len(tkn_src_str))
#       print(f"Searching based on '{tkn_src_str}'")
#       if cmd_content is not None:
#           canto_dict.show_search([cmd_content])

    ccdict_shell()
###############################################################################

if __name__ == "__main__":
    main()

