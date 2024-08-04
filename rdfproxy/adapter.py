"""SPARQLModelAdapter class for QueryResult to Pydantic model conversions."""

from collections import defaultdict
from collections.abc import Iterator
from typing import Any, overload

from SPARQLWrapper import JSON, QueryResult, SPARQLWrapper
from pydantic import BaseModel
from rdfproxy.utils._exceptions import UndefinedBindingException
from rdfproxy.utils._types import _TModelInstance
from rdfproxy.utils.sparql_templates import ungrouped_pagination_base_query
from rdfproxy.utils.utils import (
    get_bindings_from_query_result,
    instantiate_model_from_kwargs,
    temporary_query_override,
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

    def query(self) -> list[BaseModel]:
        """Run query against endpoint, map SPARQL result sets to model and return model instances."""
        return [model for model, _ in self._run_query()]

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

    @staticmethod
    def _calculate_offset(page: int, size: int) -> int:
        """Calculate offset value for paginated SPARQL templates."""
        match page:
            case 1:
                return 0
            case 2:
                return size
            case _:
                return size * page

    def _query_paginate_ungrouped(self, page: int, size: int):
        """Run query with pagination according to page and size.

        This method is intended to be part of the public SPARQLModelAdapter.query_paginate method.

        The internal query is dynamically modified according to page/offset and size/limit
        and run with SPARQLModelAdapter.query.
        """
        paginated_query = ungrouped_pagination_base_query.substitute(
            query=self._query, offset=self._calculate_offset(page, size), limit=size
        )

        with temporary_query_override(self.sparql_wrapper):
            self.sparql_wrapper.setQuery(paginated_query)
            return self.query()

    @overload
    def query_paginate(
        self, page: int, size: int, group_by: None = None
    ) -> list[BaseModel]: ...

    @overload
    def query_paginate(
        self, page: int, size: int, group_by: str
    ) -> dict[str, list[BaseModel]]: ...

    def query_paginate(
        self, page: int, size: int, group_by: str | None = None
    ) -> list[BaseModel] | dict[str, list[BaseModel]]:
        """Run query with pagination according to page and size and optional grouping."""
        if group_by is None:
            return self._query_paginate_ungrouped(page=page, size=size)
        else:
            raise NotImplementedError
