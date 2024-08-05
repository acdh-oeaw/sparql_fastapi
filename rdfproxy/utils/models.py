"""Pydantic Model definitions for rdfproxy."""

from typing import Generic

from pydantic import BaseModel
from rdfproxy.utils._types import _TModelInstance


class Page(BaseModel, Generic[_TModelInstance]):
    """Page model for rdfproxy pagination functionality.

    This model is loosely inspired by the fastapi-pagination Page class,
    see https://github.com/uriyyo/fastapi-pagination.

    Also see https://docs.pydantic.dev/latest/concepts/models/#generic-models
    for Generic Pydantic models.
    """

    items: list[_TModelInstance]
    page: int
    size: int
    total: int | None = None
    pages: int | None = None
