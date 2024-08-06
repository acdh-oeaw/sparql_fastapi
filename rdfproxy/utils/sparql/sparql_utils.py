"""Functionality for dynamic SPARQL query modifcation."""

import re


def inject_subquery(query: str, subquery: str) -> str:
    """Inject a subquery into query."""

    def _indent_query(query: str, indent: int = 2) -> str:
        """Indent a query by n spaces according to indent parameter."""
        indented_query = "".join(
            [f"{' ' * indent}{line}\n" for line in query.splitlines()]
        )
        return indented_query

    point: int = query.rfind("}")
    partial_query: str = query[:point]
    indented_subquery: str = _indent_query(subquery)

    new_query: str = f"{partial_query}  " f"{{{indented_subquery}}}\n}}"
    return new_query


def replace_query_select_clause(query: str, repl: str) -> str:
    """eplace the SELECT clause of a query with with repl."""
    if re.search(r"select\s.+", query, re.I) is None:
        raise Exception("Unable to obtain SELECT clause.")

    count_query = re.sub(
        pattern=r"select\s.+",
        repl=repl,
        string=query,
        count=1,
        flags=re.I,
    )

    return count_query


def construct_count_query(query: str) -> str:
    """Construct a generic count query from a SELECT query."""
    count_query = replace_query_select_clause(query, "select (count(*) as ?cnt)")
    return count_query


def calculate_offset(page: int, size: int) -> int:
    """Calculate offset value for paginated SPARQL templates."""
    match page:
        case 1:
            return 0
        case 2:
            return size
        case _:
            return size * page
