# RDFProxy Examples

## Basic FastAPI/RDFProxy example

Run the `basic_fastapi_example` with Poetry by executing:

```shell
poetry run fastapi dev basic_fastapi_example.py
```

### Routes

- http://localhost:8000/

  `SPARQLModelAdapter.query()` maps the flat SPARQL result sets to the nested `ComplexModel` model and returns a list of `ComplexModel` instances which are then served over the FastAPI route.

  ![<img source="basic_fastapi_example_base_route.png" width=10% height=10%>](https://github.com/acdh-oeaw/rdfproxy/blob/lupl/dev/examples/images/basic_fastapi_example_base_route.png?raw=true)


- http://localhost:8000/group/?group_by=x

  `SPARQModelAdapter.query` with the `group_by` parameter groups the result model instances according to a SPARQL binding. In this case results are grouped by the "x" binding.

  ![<img source="basic_fastapi_example_group_route.png" width=10% height=10%>](https://github.com/acdh-oeaw/rdfproxy/blob/lupl/dev/examples/images/basic_fastapi_example_group_route.png?raw=true)

  * [ ] 
- http://localhost:8000/paginate/?page=1&size=2

  Supplying `SPARQLModelAdapter.query` with arguments for the `page` and `size` parameters returns an `rdfproxy.Page` object (a generic model instance holding the actual results along the `items` field and also pagination metadata).

  ![<img source="basic_fastapi_example_paginate_route.png" width=10% height=10%>](https://github.com/acdh-oeaw/rdfproxy/blob/lupl/dev/examples/images/basic_fastapi_example_paginate_route.png?raw=true)


- http://localhost:8000/grouped_paginate/?page=1&size=2&group_by=x

  `SPARQModelAdapter.query` with arguments for `page`/`size` parameters as well as the `group_by` parameter returns an `rdfproxy.Page` object where the `items` field points to a model grouping.

  ![<img source="basic_fastapi_example_grouped_paginate_route.png" width=10% height=10%>](https://github.com/acdh-oeaw/rdfproxy/blob/lupl/dev/examples/images/basic_fastapi_example_grouped_paginate_route.png?raw=true)


