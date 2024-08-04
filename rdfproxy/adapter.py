"""SPARQLModelAdapter class for QueryResult to Pydantic model conversions."""

from collections import defaultdict
from collections.abc import Iterator
from typing import Any

from SPARQLWrapper import JSON, QueryResult, SPARQLWrapper
from pydantic import BaseModel
from rdfproxy.utils._exceptions import UndefinedBindingException
from rdfproxy.utils._types import _TModelInstance
from rdfproxy.utils.utils import (
    get_bindings_from_query_result,
    instantiate_model_from_kwargs,
)


class SPARQLModelAdapter:
    """Adapter/Mapper for QueryResult to Pydantic model conversions.

    The rdfproxy.SPARQLModelAdapter class allows to run a query against an endpoint
    and map a flat SPARQL query result set to a potentially nested Pydantic model.

    Example:

        from pydantic import BaseModel
        from rdfproxy import SPARQLModelAdapter, _TModelInstance

        class SimpleModel(BaseModel):
            x: int
            y: int

        class NestedModel(BaseModel):
            a: str
            b: SimpleModel

        class ComplexModel(BaseModel):
            p: str
            q: NestedModel

        query = '''
            select ?x ?y ?a ?p
            where {
                values (?x ?y ?a ?p) {
                    (1 2 "a value" "p value")
                }
            }
        '''

        adapter = SPARQLModelAdapter(
            endpoint="https://query.wikidata.org/bigdata/namespace/wdq/sparql",
            query=query,
            model=ComplexModel,
        )

        models: Iterator[_TModelInstance] = adapter.query()
    """

    def __init__(self, endpoint: str, query: str, model: type[_TModelInstance]) -> None:
        self._endpoint = endpoint
        self._query = query
        self._model = model

        self.sparql_wrapper = self._init_sparql_wrapper()

    def _init_sparql_wrapper(self) -> SPARQLWrapper:
        """Initialize a SPARQLWrapper object."""
        sparql_wrapper = SPARQLWrapper(self._endpoint)
        sparql_wrapper.setQuery(self._query)
        sparql_wrapper.setReturnFormat(JSON)

        return sparql_wrapper

    def _run_query(self) -> Iterator[tuple[BaseModel, dict[str, Any]]]:
        """Run the intially defined query against the endpoint using SPARQLWrapper.

        Model instances are coupled with flat SPARQL result bindings;
        this allows for easier and more efficient grouping operations (see query_group_by).
        """
        query_result: QueryResult = self.sparql_wrapper.query()
        _bindings = get_bindings_from_query_result(query_result)

        for bindings in _bindings:
            model = instantiate_model_from_kwargs(self._model, **bindings)
            yield model, bindings

    def query(self) -> Iterator[BaseModel]:
        """Run query against endpoint, map SPARQL result sets to model and return model instances."""
        for model, _ in self._run_query():
            yield model

    def query_group_by(self, group_by: str) -> dict[str, list[BaseModel]]:
        """Run query against endpoint like SPARQLModelAdapter.query but group results by a SPARQL binding.

        Example:

            from models import ComplexModel
            from rdfproxy import SPARQLModelAdapter, _TModelInstance

            query = '''
                select ?x ?y ?a ?p
                where {
                    values (?x ?y ?a ?p) {
                        (1 2 "a value" "p value")
                        (1 3 "another value" "p value 2")
                        (2 4 "yet anoter value" "p value 3")
                    }
                }
            '''

            adapter = SPARQLModelAdapter(
                endpoint="https://query.wikidata.org/bigdata/namespace/wdq/sparql",
                query=query,
                model=ComplexModel,
        )

            grouped: dict[str, list[BaseModel]] = adapter.query_group_by("x")
            assert len(grouped["1"]) == 2  # True
        """
        group = defaultdict(list)

        for model, bindings in self._run_query():
            try:
                key = bindings[group_by]
            except KeyError:
                raise UndefinedBindingException(
                    f"SPARQL binding '{group_by}' requested for grouping "
                    f"not in query projection '{bindings}'."
                )

            group[str(key)].append(model)

        return group
