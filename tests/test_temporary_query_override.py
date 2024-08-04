"""Pytest entry point for temporary_query_override"""

from SPARQLWrapper import SPARQLWrapper
from rdfproxy.utils.utils import temporary_query_override


def test_temporary_query_override_basic():
    """Basic test for temporary_query_override.

    Set a query, override it in a temporary_query_override context
    and check both SPARQLWrapper object for the currently set query.
    """
    sparql_wrapper = SPARQLWrapper(
        "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
    )
    query_1 = "select * {values ?x {1 2 3}}"
    query_2 = "select * {values ?y {11 22 33}}"

    sparql_wrapper.setQuery(query_1)

    with temporary_query_override(sparql_wrapper):
        sparql_wrapper.setQuery(query_2)
        assert sparql_wrapper.queryString == query_2

    assert sparql_wrapper.queryString == query_1
