"""SPARQL/FastAPI utils."""

from collections.abc import Iterator, Mapping
from typing import cast

from SPARQLWrapper import QueryResult
from pydantic import BaseModel
from rdfproxy.utils._types import _TModelInstance
from toolz import valmap


def get_bindings_from_query_result(query_result: QueryResult) -> Iterator[dict]:
    """Extract just the bindings from a SPARQLWrapper.QueryResult."""
    if (result_format := query_result.requestedFormat) != "json":
        raise Exception(
            "Only QueryResult objects with JSON format are currently supported. "
            f"Received object with requestedFormat '{result_format}'."
        )

    query_json = cast(Mapping, query_result.convert())
    bindings = map(
        lambda binding: valmap(lambda v: v["value"], binding),
        query_json["results"]["bindings"],
    )

    return bindings


def instantiate_model_from_kwargs(
    model: type[_TModelInstance], **kwargs
) -> _TModelInstance:
    """Instantiate a (potentially nested) model from (flat) kwargs.

    More a more generic version of this function see upto.init_model_from_kwargs
    https://github.com/lu-pl/upto?tab=readme-ov-file#init_model_from_kwargs.

    Example:

        class SimpleModel(BaseModel):
            x: int
            y: int


        class NestedModel(BaseModel):
            a: str
            b: SimpleModel


        class ComplexModel(BaseModel):
            p: str
            q: NestedModel


    model = instantiate_model_from_kwargs(ComplexModel, x=1, y=2, a="a value", p="p value")
    print(model)  # p='p value' q=NestedModel(a='a value', b=SimpleModel(x=1, y=2))
    """

    def _get_bindings(model: type[_TModelInstance], **kwargs) -> dict:
        """Get the bindings needed for model instantation.

        The function traverses model.model_fields
        and constructs a bindings dict by either getting values from kwargs or field defaults.
        For model fields the recursive clause runs.

        Note: This needs exception handling and proper testing.
        """
        return {
            k: (
                v.annotation(**_get_bindings(v.annotation, **kwargs))
                if isinstance(v.annotation, type(BaseModel))
                else kwargs.get(k, v.default)
            )
            for k, v in model.model_fields.items()
        }

    return model(**_get_bindings(model, **kwargs))
