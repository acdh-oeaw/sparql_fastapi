"""Basic example for using rdfproxy.SPARQLModelAdapter with FastAPI."""

from fastapi import FastAPI
from pydantic import BaseModel
from rdfproxy import Page, SPARQLModelAdapter


class SimpleModel(BaseModel):
    x: int
    y: int


class NestedModel(BaseModel):
    a: str
    b: SimpleModel


class ComplexModel(BaseModel):
    p: str
    q: NestedModel


query = """
select ?x ?y ?a ?p
where {
  values (?x ?y ?a ?p) {
    (1 2 "a value 1" "p value 1")
    (1 22 "a value 2" "p value 2")
    (1 222 "a value 3" "p value 3")
    (2 3 "a value 4" "p value 4")
    (2 33 "a value 5" "p value 5")
    (3 4 "a value 6" "p value 6")
    (4 5 "a value 7" "p value 7")
  }
}
"""

adapter = SPARQLModelAdapter(
    endpoint="https://query.wikidata.org/bigdata/namespace/wdq/sparql",
    query=query,
    model=ComplexModel,
)


app = FastAPI()


@app.get("/")
def base() -> list[ComplexModel]:
    return adapter.query()


@app.get("/group/")
def group(group_by: str) -> dict[str, list[ComplexModel]]:
    return adapter.query(group_by=group_by)


@app.get("/paginate/")
def paginate(page: int, size: int) -> Page[ComplexModel]:
    return adapter.query(page=page, size=size)


@app.get("/grouped_paginate/")
def grouped_paginate(page: int, size: int, group_by: str) -> Page[ComplexModel]:
    return adapter.query(page=page, size=size, group_by=group_by)
