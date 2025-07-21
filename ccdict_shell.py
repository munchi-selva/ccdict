#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""An interactive shell for searching a CantoDict dictionary."""


import click
import logging
import re
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

from ccdict.ccdict import (
    CantoDict,
    DICT_DB_FILENAME
)

from ccdict.canto_dict_types import DictField, DICT_FIELD_NAMES

def str_to_bool(bool_candidate: str) -> bool:
    """
    Converts a string to a boolean (if possible).

    Args:
        bool_candidate: A string

    Returns:
        True/False/None depending on the value of the input string
    """
    if bool_candidate.lower() in ("1", "t", "true"):
        return True
    elif bool_candidate.lower() in ("0", "f", "false"):
        return False
    return None


###############################################################################
# Shell command types enum
###############################################################################
CmdTknType = IntEnum("CmdTknType",  "CTT_NONE        \
                                     CTT_SEARCH_TERM \
                                     CTT_FIELD_LIST  \
                                     CTT_GENERAL     \
                                     CTT_COUNT",
                     start = -1)
###############################################################################


class CmdTkn(object):
    """Defines a shell command token."""
    def __init__(self,
                 tkn_type,
                 cmd_start_patt,
                 cmd_end_patt,
                 inc_start_tkn,
                 inc_end_tkn = False):
        """
        Command token constructor

        :param  tkn_type:       the token type
        :param  cmd_start_patt: pattern that marks the token's tart
        :param  cmd_end_patt:   pattern than marks the token's end
        :param  inc_start_tkn:  If true, cmd_start_patt forms part of the command content
        :param  inc_end_tkn:    If True, cmd_end_patt forms part of the command content
        """
        self.tkn_type       = tkn_type
        self.cmd_start_patt = cmd_start_patt
        self.cmd_end_patt   = cmd_end_patt
        self.inc_start_tkn  = inc_start_tkn
        self.inc_end_tkn    = inc_end_tkn
    ###########################################################################


    ###########################################################################
    def __str__(self):
        """String representation of a command token definition."""
        return "({}, {}, {}, {}, {})".format(self.tkn_type,
                                             self.cmd_start_patt,
                                             self.cmd_end_patt,
                                             self.inc_start_tkn,
                                             self.inc_end_tkn)
    ###########################################################################


    ###########################################################################
    def get_cmd_content(self,
                        tkn_src_str,
                        content_range_start,
                        content_range_end):
        """
        Returns the command content in the given range

        :param  tkn_src_str:            the command source string
        :param  content_range_start:    command content range start
        :param  content_range_end:      command content range end
        :returns the command content
        """
        return tkn_src_str[content_range_start:content_range_end].strip()
    ###########################################################################


    ###########################################################################
    def parse_tkn(self,
                  tkn_src_str,
                  tkn_start):
        """
        Parses the command token at a specified location in a source string

        :param  tkn_src_str:    the command source string
        :param  tkn_start:      token start index
        :returns the command content and the index of the token's end
        """
        tkn_end = len(tkn_src_str) - 1
        content_range_start = tkn_start + (0 if self.inc_start_tkn else 1)
        content_range_end   = len(tkn_src_str)

        #
        # Identify the end of the command token, and extract its content
        #
        end_tkn_match = re.search(self.cmd_end_patt, tkn_src_str[tkn_start+1:])
        if end_tkn_match:
            tkn_end = tkn_start + end_tkn_match.span()[1]
            if self.inc_end_tkn:
                content_range_end = tkn_end
            else:
                content_range_end = tkn_start + end_tkn_match.span()[0] + 1
        cmd_content = self.get_cmd_content(tkn_src_str, content_range_start, content_range_end)

        return cmd_content, tkn_end
###############################################################################


###############################################################################
# Command token subclass for parsing a dictionary search term
###############################################################################
class DictSearchTermCmdTkn(CmdTkn):
    def get_cmd_content(self,
                        tkn_src_str,
                        content_range_start,
                        content_range_end):
        cmd_content = None
        raw_content = super().get_cmd_content(tkn_src_str, content_range_start, content_range_end)

        field_group_name = "search_field"
        value_group_name = "search_value"
        re_search_group_name = "search_with_re"

        search_term_patt = fr"(?P<{field_group_name}>\w+)\s+'?(?P<{value_group_name}>[^']+)'?(\s+(?P<{re_search_group_name}>\w+))?"

        search_val_match = re.match(search_term_patt, raw_content)
        if search_val_match is not None:
            match_groups = search_val_match.groupdict()
            search_field = match_groups[field_group_name]
            search_value = match_groups[value_group_name]
            search_with_re = match_groups[re_search_group_name] or None

            cmd_content = DictSearchTerm(search_value, search_field=eval(search_field), use_re=search_with_re)

        return cmd_content
###############################################################################


###############################################################################
# Command token subclass for parsing a dictionary field list
###############################################################################
class FldListCmdTkn(CmdTkn):
    def get_cmd_content(self,
                        tkn_src_str,
                        content_range_start,
                        content_range_end):
        raw_cmd_content = super().get_cmd_content(tkn_src_str, content_range_start, content_range_end)
        return [eval(field_name) for field_name in raw_cmd_content.split()]
###############################################################################


###############################################################################
# Command tokens for searching the dictionary
###############################################################################
SEARCH_CMD_TOKENS: List[CmdTkn] = \
[
    DictSearchTermCmdTkn(CmdTknType.CTT_SEARCH_TERM, r"\(", r"\)", False),
    FldListCmdTkn(CmdTknType.CTT_FIELD_LIST, r"\[", r"\]", False),
    CmdTkn(CmdTknType.CTT_GENERAL, r"\"", r"\"", False),
    CmdTkn(CmdTknType.CTT_GENERAL, r"[^\s]", r"[\s]", True)
]
###############################################################################


###############################################################################
def parse_dict_search_cmd(cmd: str,
                          cmd_tkn_defs: List[CmdTkn] = SEARCH_CMD_TOKENS) -> Dict:
    # type (str) -> Dict
    """
    Parses the components of a command for a dictionary search.

    :param  cmd:    The command
    :returns a mapping between command component names and values
    """
    search_expr = None
    cmd_comps = dict()
    if cmd:
        #
        # Parse the command character by character...
        #
        tkn_start = 0
        while tkn_start < len(cmd):
            #
            # Identify the latest token's definition
            #
            tkn_def_matches = [defn for defn in cmd_tkn_defs if re.search(defn.cmd_start_patt, cmd[tkn_start])]
            tkn_def = tkn_def_matches[0] if len(tkn_def_matches) > 0 else None

            if tkn_def:
                cmd_content, tkn_end = tkn_def.parse_tkn(cmd, tkn_start)
                if tkn_def.tkn_type == CmdTknType.CTT_FIELD_LIST:
                    cmd_comps["fields"] = cmd_content
                elif tkn_def.tkn_type == CmdTknType.CTT_SEARCH_TERM:
                    search_expr = cmd_comps.get("search_expr", list())
                    if isinstance(search_expr, list):
                        search_expr.append(cmd_content)
                        cmd_comps["search_expr"] = search_expr
                else:
                    if str_to_bool(cmd_content) is not None:
                        search_expr = cmd_comps.get("search_expr", None)
                        if (not search_expr or isinstance(search_expr, str)) and not "use_re" in cmd_comps:
                            cmd_comps["use_re"] = str_to_bool(cmd_content)
                        elif not "flatten_pinyin" in cmd_comps:
                            cmd_comps["flatten_pinyin"] = str_to_bool(cmd_content)
                        elif not "compact" in cmd_comps:
                            cmd_comps["compact"] = str_to_bool(cmd_content)
                    else:
                        if cmd_content in DICT_FIELD_NAMES:
                            cmd_comps["search_field"] = eval(cmd_content)
                        elif not search_expr:
                            if cmd[tkn_start] == '"' and not "use_re" in cmd_comps:
                                #
                                # Unless regular expression usage has been explicitly
                                # enabled/disabled, treat a quoted string as a
                                # regular expression that should be matched in full,
                                # i.e. search for ^<search_expr>$
                                #
                                cmd_comps["search_expr"] = "^" + cmd_content + "$"
                                cmd_comps["use_re"] = True
                            else:
                                cmd_comps["search_expr"] = cmd_content
                        elif not "indent_str" in cmd_comps:
                            cmd_comps["indent_str"] = cmd_content
                tkn_start = tkn_end + 1
            else:
                #
                # Advance position in the command
                #
                tkn_start += 1

    return cmd_comps
###############################################################################


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


@click_group_with_default(prompt="ccdict $ ", debug=False)
@click.pass_context
@click.option("-f", "--force-reload", is_flag=True, default=False)
def ccdict_shell(ctx: click.Context, force_reload: bool):
    ctx.allow_extra_args = True
    ctx.ignore_unknown_options = True
    # Ensure that ctx.obj exists and is a dict
    ctx.ensure_object(dict)

    # Use ctx.obj to store the dictionary
    ctx.obj["dictionary"] = CantoDict(DICT_DB_FILENAME, force_reload=force_reload)
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
@click.option("--flatten/--not-flatten", default=True, help="Treat two definitions for a DF_TRAD value as the same even if their DF_PINYIN values differ")

# Search result display options
@click.option("-d", "--display-field",
              type=click.Choice(DICT_FIELD_NAMES), multiple=True, default=[DictField.DF_TRAD.name, DictField.DF_CJCODE.name, DictField.DF_JYUTPING.name, DictField.DF_ENGLISH.name],
              help="Include the specified field in the search output")
@click.option("-c", "--compact", is_flag=True, default=False, help="Compact the search result to a single line")
@click.option("-f", "--output-format", type=click.Choice(["ASCII", "JSON"]), default="ASCII", help="Format of the search result")
def search(ctx: click.Context, search_term: str,
           all: bool,
           lazy: bool,
           re: Optional[bool],
           flatten: bool,
           display_field: list[str],
           compact: bool,
           output_format: str) -> List[str]: #None:
    cmd_comps =  parse_dict_search_cmd(search_term,
                                       cmd_tkn_defs = SEARCH_CMD_TOKENS)
    cmd_comps["try_all_fields"] = all
    cmd_comps["lazy_eval"] = lazy
    cmd_comps["use_re"] = re
    cmd_comps["flatten_pinyin"] = flatten
    cmd_comps["fields"] = [DictField[field_name] for field_name in display_field]
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

#   canto_dict = CantoDict(DICT_DB_FILENAME)

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

