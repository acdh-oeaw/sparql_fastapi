"""Pytest entry point for rdfproxy.SPARQLModelAdapter tests."""

import pytest

from rdfproxy import SPARQLModelAdapter
from tests.data.models import ComplexModel


@pytest.mark.remote
def test_sparql_model_adapter_basic():
    """Simple base test for SPARQLModelAdapter."""
    query = """
    select ?x ?y ?a ?p
    where {
        values (?x ?y ?a ?p) {
            (1 2 "a value" "p value")
        }
    }
    """
    adapter = SPARQLModelAdapter(
        endpoint="https://query.wikidata.org/bigdata/namespace/wdq/sparql",
        query=query,
        model=ComplexModel,
    )

    assert all(isinstance(model, ComplexModel) for model in adapter.query())
