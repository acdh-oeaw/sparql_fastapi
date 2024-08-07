"""SPARQLModelAdapter class for SPARQL query result set to Pydantic model conversions."""

from collections import defaultdict
from collections.abc import Iterator
import math
from typing import Any, Generic, overload

from typeguard import typechecked

from SPARQLWrapper import QueryResult
from rdfproxy.utils._exceptions import (
    InterdependentParametersException,
    UndefinedBindingException,
)
from rdfproxy.utils._types import _TModelInstance
from rdfproxy.utils.models import Page
from rdfproxy.utils.sparql.sparql_templates import ungrouped_pagination_base_query
from rdfproxy.utils.sparql.sparql_utils import (
    calculate_offset,
    construct_count_query,
    construct_grouped_count_query,
    construct_grouped_pagination_query,
    init_sparql_wrapper,
    query_with_wrapper,
    temporary_query_override,
)
from rdfproxy.utils.utils import (
    get_bindings_from_query_result,
    instantiate_model_from_kwargs,
)


@typechecked
class SPARQLModelAdapter(Generic[_TModelInstance]):
    """Adapter/Mapper for SPARQL query result set to Pydantic model conversions.

    The rdfproxy.SPARQLModelAdapter class allows to run a query against an endpoint,
    map a flat SPARQL query result set to a potentially nested Pydantic model and
    optionally paginate and/or group the results by a SPARQL binding.
    """

    def __init__(self, endpoint: str, query: str, model: type[_TModelInstance]) -> None:
        self._endpoint = endpoint
        self._query = query
        self._model = model

        self.sparql_wrapper = init_sparql_wrapper(endpoint, query)

    @overload
    def query(self) -> list[_TModelInstance]: ...

    @overload
    def query(
        self,
        *,
        group_by: str,
    ) -> dict[str, list[_TModelInstance]]: ...

    @overload
    def query(
        self,
        *,
        page: int,
        size: int,
    ) -> Page[_TModelInstance]: ...

    @overload
    def query(
        self,
        *,
        page: int,
        size: int,
        group_by: str,
    ) -> Page[_TModelInstance]: ...

    def query(
        self,
        *,
        page: int | None = None,
        size: int | None = None,
        group_by: str | None = None,
    ) -> (
        list[_TModelInstance] | dict[str, list[_TModelInstance]] | Page[_TModelInstance]
    ):
        """Run query against endpoint and map the SPARQL query result set to a Pydantic model.

        Optional pagination and/or grouping by a SPARQL binding is avaible by
        supplying the group_by and/or page/size parameters.
        """
        match page, size, group_by:
            case None, None, None:
                return self._query_collect_models()
            case int(), int(), None:
                return self._query_paginate_ungrouped(page=page, size=size)
            case None, None, str():
                return self._query_group_by(group_by=group_by)
            case int(), int(), str():
                return self._query_paginate_grouped(
                    page=page, size=size, group_by=group_by
                )
            case (None, int(), Any()) | (int(), None, Any()):
                raise InterdependentParametersException(
                    "Parameters 'page' and 'size' are mutually dependent."
                )
            case _:
                raise Exception("This should never happen.")

    def _query_generate_model_bindings_mapping(
        self, query: str | None = None
    ) -> Iterator[tuple[_TModelInstance, dict[str, Any]]]:
        """Run query, construct model instances and generate a model-bindings mapping.

        The query parameter defaults to the initially defined query and
        is run against the endpoint defined in the SPARQLModelAdapter instance.

        Note: The coupling of model instances with flat SPARQL results
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

    def _query_collect_models(self, query: str | None = None) -> list[_TModelInstance]:
        """Run query against endpoint and collect model instances."""
        return [
            model
            for model, _ in self._query_generate_model_bindings_mapping(query=query)
        ]

    def _query_group_by(
        self, group_by: str, query: str | None = None
    ) -> dict[str, list[_TModelInstance]]:
        """Run query against endpoint and group results by a SPARQL binding."""
        group = defaultdict(list)

        for model, bindings in self._query_generate_model_bindings_mapping(query):
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
        result = query_with_wrapper(query=query, sparql_wrapper=self.sparql_wrapper)
        return int(next(result)["cnt"])

    def _query_paginate_ungrouped(self, page: int, size: int) -> Page[_TModelInstance]:
        """Run query with pagination according to page and size.

        The internal query is dynamically modified according to page (offset)/size (limit)
        and run with SPARQLModelAdapter._query_collect_models.
        """
        paginated_query = ungrouped_pagination_base_query.substitute(
            query=self._query, offset=calculate_offset(page, size), limit=size
        )
        count_query = construct_count_query(self._query)

        items = self._query_collect_models(query=paginated_query)
        total = self._get_count(count_query)
        pages = math.ceil(total / size)

        return Page(items=items, page=page, size=size, total=total, pages=pages)

    def _query_paginate_grouped(
        self, page: int, size: int, group_by: str
    ) -> Page[_TModelInstance]:
        """Run query with pagination according to page/size and group result by a SPARQL binding.

        The internal query is dynamically modified according to page (offset)/size (limit)
        and run with SPARQLModelAdapter._query_group_by.
        """
        grouped_paginated_query = construct_grouped_pagination_query(
            query=self._query, page=page, size=size, group_by=group_by
        )
        grouped_count_query = construct_grouped_count_query(
            query=self._query, group_by=group_by
        )

        items = self._query_group_by(group_by=group_by, query=grouped_paginated_query)
        total = self._get_count(grouped_count_query)
        pages = math.ceil(total / size)

        return Page(items=items, page=page, size=size, total=total, pages=pages)
