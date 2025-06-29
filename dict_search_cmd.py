import cmd                  # Command line interpreter support
import logging
from collections import namedtuple

from ccdict import CantoDict, DICT_DB_FILENAME, parse_dict_search_cmd

dict_search_logger = logging.getLogger(__name__)

OptDef = namedtuple("OptDef",
                    "name data_type default eval")

class DictSearchCmd(cmd.Cmd):
    """Legacy interactive shell for searching a Canto dict."""
    intro = "Cantonese dictionary search shell"
    prompt = "ccdict> "
    SET_CMD = "set"
    QUIT_CMD = "q"
    HELP_CMD = "?"
    dictionary = CantoDict(DICT_DB_FILENAME)

    std_opts = dict()
    std_opts["try_all_fields"]  = True
    std_opts["lazy_eval"]       = True

    DICT_OPT_SRCH_TRY_ALL   = "try_all_fields"
    DICT_OPT_SRCH_LAZY      = "lazy_eval"
    DICT_OPT_SRCH_FLATTEN   = "flatten_pinyin"
    DICT_OPT_SRCH_USE_RE    = "use_re"
    DICT_OPT_SRCH_FLD       = "search_field"
    DICT_OPT_DISP_COMPACT   = "compact"
    DICT_OPT_DISP_FLDS      = "fields"
    DICT_OPT_DISP_INDENT    = "indent_str"

    OPT_DEFS = [OptDef(DICT_OPT_SRCH_TRY_ALL,   "bool", True, False),
                OptDef(DICT_OPT_SRCH_LAZY,      "bool", True, False),
                OptDef(DICT_OPT_SRCH_FLATTEN,   "bool", True, False),
                OptDef(DICT_OPT_SRCH_USE_RE,    "bool", None, False),
                OptDef(DICT_OPT_SRCH_FLD,       "str",  None, True),
                OptDef(DICT_OPT_DISP_COMPACT,   "bool", False, False),
                OptDef(DICT_OPT_DISP_FLDS,      "list", ["DE_FLD_TRAD", "DE_FLD_CJCODE", "DE_FLD_JYUTPING", "DE_FLD_ENGLISH"], True),
                OptDef(DICT_OPT_DISP_INDENT,    "str",  "", False)]

    def __init__(self):
        super().__init__()
        self.settings = dict()
        for opt_def in DictSearchCmd.OPT_DEFS:
            self.settings[opt_def.name] = {"def": opt_def}

    def do_quit(self, arg):
        return True

    def do_help(self, arg):
        cmd_indent = "\t"
        opt_indent = "\t    "
        topic_field = "field"

        help_string = ""
        if arg == topic_field:
            help_string = "Valid dictionary entry fields\n" + \
                          cmd_indent + "DE_FLD_TRAD:\ttraditional Chinese\n" + \
                          cmd_indent + "DE_FLD_SIMP:\tsimplified Chinese\n" + \
                          cmd_indent + "DE_FLD_JYUTPING:\tJyutping transcription\n" + \
                          cmd_indent + "DE_FLD_PINYIN:\tPinyin transcription\n" + \
                          cmd_indent + "DE_FLD_ENGLISH:\tEnglish definition\n" + \
                          cmd_indent + "DE_FLD_CJCODE:\tChangJie code"
        else:
            help_string = "usage: " + \
                            cmd_indent + "search_term [search_field] [use_regex] [output_field1 ...]\n" + \
                            cmd_indent + "search_exp1 [search_exp2 ...] [output_field1 ...]\n" + \
                            opt_indent + "search_term: search string\n" + \
                            opt_indent + "search_field: field to match against search_term\n" + \
                            opt_indent + "search_exp1, search_exp2, etc.: search_val | (search_val search_field [use_regex])\n" + \
                          "Help topics: \n" + \
                            cmd_indent + "? field (search/output field options)"
        print(help_string)

    def do_set(self, arg):
        set_string = arg.strip() if arg else str()

        # Split the setting command into option name and values
        (opt_name, _, opt_val) = set_string.partition(" ")
        opt_val = opt_val.strip()

        opts_to_show = list()

        if not opt_name:
            opts_to_show = self.settings.keys()
        elif opt_name not in self.settings:
            print("Unrecognised option: {}".format(opt_name))
            return
        elif not opt_val:
            opts_to_show.append(opt_name)

        if opts_to_show:
            for opt_name in opts_to_show:
                # Print setting(s)
                opt_setting = self.settings[opt_name]
                print("{} = {}".format(opt_name, opt_setting.get("value", opt_setting["def"].default)))
            return

        opt_setting = self.settings[opt_name]
        opt_def = opt_setting["def"]
        opt_type = opt_def.data_type
        if opt_type == "bool":
            opt_val = str_to_bool(opt_val)
        elif opt_type == "list":
            opt_val = opt_val.split()

        dict_search_logger.log(logging.INFO, "BEFORE settings")
        pprint(self.settings)

        print("Setting: '{}'".format(opt_name))
        print("\tto: '{}'".format(opt_val))
        opt_setting["value"] = opt_val

        dict_search_logger.log(logging.INFO, "AFTER settings")
        pprint(self.settings)

    def do_search(self, arg):
        for opt_name in self.settings:
            if not opt_name in self.cmd_comps:

                opt_setting = self.settings[opt_name]
                opt_def = opt_setting["def"]
                opt_type = opt_def.data_type
                opt_val = opt_setting.get("value", opt_def.default)

                if opt_type == "list":
                    self.cmd_comps[opt_name] = list()
                    for val in opt_val:
                        if opt_def.eval:
                            val = eval(val)
                        self.cmd_comps[opt_name].append(val)
                elif opt_type == "str" and opt_def.eval:
                    if opt_val:
                        self.cmd_comps[opt_name] = eval(opt_val)
                else:
                    self.cmd_comps[opt_name] = opt_val

        search_expr = self.cmd_comps.pop("search_expr", None)
        print(f"Search expression is {search_expr}")
        if search_expr:
            self.dictionary.show_search(search_expr, **(self.cmd_comps))

    def precmd(self, line):
        line = line.strip()

        if line == self.QUIT_CMD:
            return "quit"

        line_tokens = line.split()

        if len(line_tokens) > 0:
            if line_tokens[0] == self.HELP_CMD or line_tokens[0] == self.SET_CMD:
                # Allow these commands to be passed in full to respective do_*() methods
                return line

        self.cmd_comps = parse_dict_search_cmd(line)
        return "search"


def main():
    """Starts the dictionary searching interactive shell."""

    DictSearchCmd().cmdloop()


if __name__ == "__main__":
    main()
