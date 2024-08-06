"""SPARQLModelAdapter class for QueryResult to Pydantic model conversions."""

from collections import defaultdict
from collections.abc import Iterator
import math
import re
from typing import Any, overload
from typing import Generic

from SPARQLWrapper import JSON, QueryResult, SPARQLWrapper
from rdfproxy.utils._exceptions import UndefinedBindingException
from rdfproxy.utils._types import _TModelInstance
from rdfproxy.utils.models import Page
from rdfproxy.utils.sparql.sparql_templates import ungrouped_pagination_base_query
from rdfproxy.utils.sparql.sparql_utils import calculate_offset, construct_count_query
from rdfproxy.utils.utils import (
    get_bindings_from_query_result,
    instantiate_model_from_kwargs,
    temporary_query_override,
)


class SPARQLModelAdapter(Generic[_TModelInstance]):
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

        models: Iterator[ComplexModel] = adapter.query()
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

    def _run_query(self) -> Iterator[tuple[_TModelInstance, dict[str, Any]]]:
        """Run the initially defined query against the endpoint using SPARQLWrapper.

        Model instances are coupled with flat SPARQL result bindings;
        this allows for easier and more efficient grouping operations (see query_group_by).
        """
        query_result: QueryResult = self.sparql_wrapper.query()
        _bindings = get_bindings_from_query_result(query_result)

        for bindings in _bindings:
            model = instantiate_model_from_kwargs(self._model, **bindings)
            yield model, bindings

    def query(self) -> list[_TModelInstance]:
        """Run query against endpoint, map SPARQL result sets to model and return model instances."""
        return [model for model, _ in self._run_query()]

    def query_group_by(self, group_by: str) -> dict[str, list[_TModelInstance]]:
        """Run query against endpoint and group results by a SPARQL binding."""
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

    def _get_count(self) -> int:
        """Construct a count query from the initialized query, run it and return the count result."""
        count_query = construct_count_query(self.sparql_wrapper.queryString)

        with temporary_query_override(self.sparql_wrapper):
            self.sparql_wrapper.setQuery(count_query)
            result = get_bindings_from_query_result(self.sparql_wrapper.query())

        return int(next(result)["cnt"])

    def _query_paginate_ungrouped(self, page: int, size: int) -> Page[_TModelInstance]:
        """Run query with pagination according to page and size.

        This method is intended to be part of the public SPARQLModelAdapter.query_paginate method.

        The internal query is dynamically modified according to page/offset and size/limit
        and run with SPARQLModelAdapter.query.
        """
        paginated_query = ungrouped_pagination_base_query.substitute(
            query=self._query, offset=calculate_offset(page, size), limit=size
        )

        total = self._get_count()
        pages = math.ceil(total / size)

        with temporary_query_override(self.sparql_wrapper):
            self.sparql_wrapper.setQuery(paginated_query)
            items: list[_TModelInstance] = self.query()

        return Page(items=items, page=page, size=size, total=total, pages=pages)

    @overload
    def query_paginate(
        self, page: int, size: int, group_by: None = None
    ) -> Page[_TModelInstance]: ...

    @overload
    def query_paginate(
        self, page: int, size: int, group_by: str
    ) -> dict[str, list[_TModelInstance]]: ...

    def query_paginate(
        self, page: int, size: int, group_by: str | None = None
    ) -> Page[_TModelInstance] | dict[str, list[_TModelInstance]]:
        """Run query with pagination according to page and size and optional grouping."""
        if group_by is None:
            return self._query_paginate_ungrouped(page=page, size=size)
        else:
            raise NotImplementedError
