"""SPARQLModelAdapter class for QueryResult to Pydantic model conversions."""

from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import Any, cast, overload

from SPARQLWrapper import JSON, QueryResult, SPARQLWrapper
from pydantic import BaseModel
from rdfproxy.utils._types import _TModelConstructorCallable, _TModelInstance
from rdfproxy.utils.utils import (
    get_bindings_from_query_result,
    instantiate_model_from_kwargs,
)


class SPARQLModelAdapter:
    """Adapter/Mapper for QueryResult to Pydantic model conversions.

    The rdfproxy.SPARQLModelAdapter class allows to run a query against an endpoint
    and map a flat SPARQL query result set to a potentially nested Pydantic model.

    Example:

        from SPARQLWrapper import SPARQLWrapper
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
