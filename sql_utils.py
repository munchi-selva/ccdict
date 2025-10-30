#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""General SQL DB (sqlite) utility functions."""

import re
import sqlite3
from pprint import pprint

def regexp(pattern: str, field: str) -> bool:
    """
    Implements the user function called by sqlite's REGEXP operator.

    Allows WHERE clause conditions that perform regular expression matching.

    See https://www.sqlite.org/lang_expr.html#regexp

    Args:
        pattern:    Regular expression to be matched
        field:      Field being regular expression tested

    Returns:
        True if field matches pattern.
    """
    re_pattern = re.compile(pattern)
    return field and re_pattern.search(field) is not None


def table_exists(sqlcur: sqlite3.Cursor, table_name: str) -> int:
    """
    Checks if a table with the given name exists.

    Args:
        sqlcur:     Cursor instance for running queries
        table_name: Name of the table

    Returns:
        True if the table exists
    """
    sqlcur.execute("SELECT COUNT(*) AS table_count FROM sqlite_master WHERE name = ?",
                   (table_name,))
    return (sqlcur.fetchone()[0] != 0)


def row_count(sqlcur: sqlite3.Cursor, table_name: str) -> int:
    """Returns the row count for a given table.

    Args:
        sqlcur:     Cursor instance for running queries
        table_name: Name of the table

    Returns:
        The number of rows in the table
    """
    sqlcur.execute(f"SELECT COUNT(*) AS [{table_name} rows] FROM {table_name}")
    return sqlcur.fetchone()[0]


def show_query(sqlcur: sqlite3.Cursor, query: str, as_dict: bool = False) -> None:
    """Shows the results of a given query.

    Args:
        sqlcur:     Cursor instance for running queries
        query:      The query
        as_dict:    If True, displays each row returned as a dictionary

    Returns:
        Nothing
    """
    pprint(f"{query}")
    if as_dict:
        pprint([dict(row) for row in sqlcur.execute(query)])
    else:
        pprint([tuple(row) for row in sqlcur.execute(query)])
