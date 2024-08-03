"""Pytest entry point for rdfproxy.get_bindings_query_result tests."""

from collections.abc import Iterator

from SPARQLWrapper import QueryResult
import pytest
from rdfproxy import get_bindings_from_query_result


@pytest.mark.remote
def test_get_bindings_from_query_result_basic(wikidata_wrapper):
    """Simple base test for get_bindings_from_query_result.

    Run a VALUES query and check keys and values of the bindings dict.
    """
    query = """
    select ?x ?y ?a ?p
    where {
        values (?x ?y ?a ?p) {
            (1 2 "a value" "p value")
        }
    }
    """

    wikidata_wrapper.setQuery(query)
    result: QueryResult = wikidata_wrapper.query()

    bindings: Iterator[dict] = get_bindings_from_query_result(result)
    binding = next(bindings)

    assert all(var in binding.keys() for var in ["x", "y", "a", "p"])
    assert all(value in binding.values() for value in ["1", "2", "a value", "p value"])
