"""Functionality for dynamic SPARQL query modifcation."""

from collections.abc import Iterator
from contextlib import contextmanager
import re
from typing import Any

from SPARQLWrapper import JSON, QueryResult, SPARQLWrapper
from rdfproxy.utils.sparql.sparql_templates import ungrouped_pagination_base_query
from rdfproxy.utils.utils import get_bindings_from_query_result


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
    """Replace the SELECT clause of a query with repl."""
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
            return size * (page - 1)


def construct_grouped_pagination_query(
    query: str, page: int, size: int, group_by: str
) -> str:
    """Dynamically construct a query for grouped pagination.

    Based on the initial query, construct a query with limit/offset according to page/size
    and with a SELECT clause that distinctly selects the group_by variable;
    then inject that query into the initial query as a subquery.
    """
    _paginated_query = ungrouped_pagination_base_query.substitute(
        query=query, offset=calculate_offset(page, size), limit=size
    )
    subquery = replace_query_select_clause(
        _paginated_query, f"select distinct ?{group_by}"
    )

    grouped_pagination_query = inject_subquery(query=query, subquery=subquery)
    return grouped_pagination_query


def construct_grouped_count_query(query: str, group_by) -> str:
    grouped_count_query = replace_query_select_clause(
        query, f"select (count(distinct ?{group_by}) as ?cnt)"
    )

    return grouped_count_query


def init_sparql_wrapper(endpoint: str, query: str, return_format: str = JSON):
    """Initialize a SPARQLWrapper object."""
    sparql_wrapper = SPARQLWrapper(endpoint)
    sparql_wrapper.setQuery(query)
    sparql_wrapper.setReturnFormat(return_format)

    return sparql_wrapper


@contextmanager
def temporary_query_override(sparql_wrapper: SPARQLWrapper):
    """Context manager that allows to contextually overwrite a query in a SPARQLWrapper object."""
    _query_cache = sparql_wrapper.queryString

    try:
        yield sparql_wrapper
    finally:
        sparql_wrapper.setQuery(_query_cache)


def query_with_wrapper(query: str, sparql_wrapper: SPARQLWrapper) -> Iterator[dict]:
    """Execute a SPARQL query using a predefined sparql_wrapper object.

    The query attribute of the wrapper object is temporarily overridden
    and gets restored after query execution.
    """
    with temporary_query_override(sparql_wrapper=sparql_wrapper):
        sparql_wrapper.setQuery(query)
        result: QueryResult = sparql_wrapper.query()

    bindings: Iterator[dict] = get_bindings_from_query_result(result)
    return bindings
