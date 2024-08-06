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
from rdfproxy.utils.sparql.sparql_utils import (
    calculate_offset,
    construct_count_query,
    construct_grouped_count_query,
    construct_grouped_pagination_query,
    init_sparql_wrapper,
    sparql_wrapper_query,
)
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

        self.sparql_wrapper = init_sparql_wrapper(endpoint, query)

    def _run_query(
        self, query: str | None = None
    ) -> Iterator[tuple[_TModelInstance, dict[str, Any]]]:
        """Run query, construct model instances and generate a model-bindings mapping.

        Query defaults to the initially defined query
        and is run against the endpoint defined in the SPARQLModelAdapter instance.

        The coupling of model instances with flat SPARQL results
        allows for easier and more efficient grouping operations (see grouping functionality).
        """
        if query is None:
            query_result: QueryResult = self.sparql_wrapper.query()
        else:
            with temporary_query_override(self.sparql_wrapper):
                self.sparql_wrapper.setQuery(query)
                query_result: QueryResult = self.sparql_wrapper.query()

        _bindings = get_bindings_from_query_result(query_result)

        for bindings in _bindings:
            model = instantiate_model_from_kwargs(self._model, **bindings)
            yield model, bindings

    def query(self, query: str | None = None) -> list[_TModelInstance]:
        """Run query against endpoint and collect model instances."""
        return [model for model, _ in self._run_query(query=query)]

    def query_group_by(
        self, group_by: str, query: str | None = None
    ) -> dict[str, list[_TModelInstance]]:
        """Run query against endpoint and group results by a SPARQL binding."""
        group = defaultdict(list)

        for model, bindings in self._run_query(query):
            try:
                key = bindings[group_by]
            except KeyError:
                raise UndefinedBindingException(
                    f"SPARQL binding '{group_by}' requested for grouping "
                    f"not in query projection '{bindings}'."
                )

            group[str(key)].append(model)

        return group

    def _get_count(self, query: str) -> int:
        """Construct a count query from the initialized query, run it and return the count result."""
        result = sparql_wrapper_query(query=query, sparql_wrapper=self.sparql_wrapper)
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
        count_query = construct_count_query(self._query)

        items = self.query(query=paginated_query)
        total = self._get_count(count_query)
        pages = math.ceil(total / size)

        return Page(items=items, page=page, size=size, total=total, pages=pages)

    def _query_paginate_grouped(
        self, page: int, size: int, group_by: str
    ) -> Page[_TModelInstance]:
        grouped_pagination_query = construct_grouped_pagination_query(
            query=self._query, page=page, size=size, group_by=group_by
        )
        grouped_count_query = construct_grouped_count_query(
            query=self._query, group_by=group_by
        )

        items = self.query_group_by(group_by=group_by, query=grouped_pagination_query)
        total = self._get_count(grouped_count_query)
        pages = math.ceil(total / size)
        return Page(items=items, page=page, size=size, total=total, pages=pages)

    def query_paginate(
        self, page: int, size: int, group_by: str | None = None
    ) -> Page[_TModelInstance]:
        """Run query with pagination according to page and size and optional grouping."""
        if group_by is None:
            return self._query_paginate_ungrouped(page=page, size=size)
        else:
            return self._query_paginate_grouped(page=size, size=size, group_by=group_by)
