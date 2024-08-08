from rdfproxy.adapter import SPARQLModelAdapter
from rdfproxy.utils._types import _TModelConstructorCallable, _TModelInstance
from rdfproxy.utils.models import Page
from rdfproxy.utils.utils import (
    get_bindings_from_query_result,
    instantiate_model_from_kwargs,
)
