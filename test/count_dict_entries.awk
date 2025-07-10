#
# Gets the number of dictionary entries in a CC-* dictionary file.
#
# If run with verbose=1, outputs the number of entries per entry key
# (traditional Chinese value).
# Otherwise, only outputs the total entry count.
#

# Gets the number of dictionary entries in a CC-* dictionary file line
function get_dict_entry_count(dict_line,    jyutping_count, english_def_count) {
    if (match(dict_line, /({.+})[[:space:]]+(\/.*)/, line_components)) {
        # cccanto format
        #
        # Each line can contain multiple Jyutping transcriptions and multiple
        # English translations.
        #
        # The following pattern is more accurate: /({[^}]+})[[:space:]]+(.*)/
        # Lazy matching of the {} is used to compensate for an error in my
        # current cccanto file:
        #   陳慧琳 陈慧琳 [chen2 hui4 lin2] {can4 wai6} lam4} /Kelly Chen Wai Lam, a Hong Kong singer/

        split(line_components[1], jyutping_elems, "/")
        jyutping_count = length(jyutping_elems)

        split(line_components[2], english_defs, "/")
        for (i in english_defs) {
            english_def_count += english_defs[i] ~ /^[[:space:]]*[^#[:space:]].*/
        }
        return jyutping_count * english_def_count
    }
    else if (match(dict_line, /\[[^]]+\][[:space:]]+(\/.*)/, line_components)) {
        # cedict format
        split(line_components[1], english_defs, "/")
        for (i in english_defs) {
            english_def_count += english_defs[i] ~ /^[[:space:]]*[^#[:space:]].*/
        }
        return english_def_count
    }
    else if (match(dict_line, /\[[^]]+\][[:space:]]+{[^}]+}[[:space:]]*/, line_components)) {
        return 1
        # cedict-cccanto format
    }

    return 0
}

/^[^#]/ {
    entry_count = get_dict_entry_count($0)
    total_entry_count += entry_count
    dict_entries[$1] += entry_count
}

END {
    for (entry_key in dict_entries) {
        if (verbose)
            printf "%s\t%s\n", entry_key, dict_entries[entry_key]
    }

    print total_entry_count
}
