"""Pytest entry point for rdfproxy.SPARQLModelAdapter tests."""

import pytest

from hypothesis import given, settings
from rdfproxy import SPARQLModelAdapter
from rdfproxy.utils._exceptions import UndefinedBindingException
from tests.data.models import ComplexModel
from tests.utils.strategies.general_strategies import public_variable_names


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


@pytest.mark.remote
def test_sparql_model_adapter_grouping_basic():
    query = """
    select ?x ?y ?a ?p ?z
    where {
        values (?x ?y ?a ?p ?z) {
           (1 2 "a value" "p value" 3)
           (1 22 "other value" "yet another value" 3)
           (2 22 "other value" "yet another value" 3)
          }
    }
    """

    adapter = SPARQLModelAdapter(
        endpoint="https://query.wikidata.org/bigdata/namespace/wdq/sparql",
        query=query,
        model=ComplexModel,
    )

    group_x = adapter.query_group_by("x")
    group_y = adapter.query_group_by("y")
    group_z = adapter.query_group_by("z")

    assert {1, 2} == set(len(v) for _, v in group_x.items())
    assert {1, 2} == set(len(v) for _, v in group_y.items())
    assert {3} == set(len(v) for _, v in group_z.items())


@pytest.mark.remote
@given(var=public_variable_names)
@settings(max_examples=10)
def test_sparql_model_adapter_grouping_basic_fail(var):
    query = """
    select ?x ?y ?a ?p ?z
    where {
        values (?x ?y ?a ?p ?z) {
           (1 2 "a value" "p value" 3)
           (1 22 "other value" "yet another value" 3)
           (2 22 "other value" "yet another value" 3)
          }
    }
    """

    adapter = SPARQLModelAdapter(
        endpoint="https://query.wikidata.org/bigdata/namespace/wdq/sparql",
        query=query,
        model=ComplexModel,
    )

    if var not in ["x", "y", "a", "p", "z"]:
        with pytest.raises(UndefinedBindingException):
            group = adapter.query_group_by(var)  # noqa: F841
