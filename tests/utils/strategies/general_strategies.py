"""General Hypothesis strategies for testing RDFProxy."""

import keyword
from typing import Annotated

from hypothesis import strategies as st
from hypothesis.strategies._internal.strategies import SearchStrategy


public_variable_names: Annotated[
    SearchStrategy, "Strategy for public Python variable names"
] = st.from_regex(r"^[a-z]+[a-z_]*$", fullmatch=True).filter(
    lambda x: not keyword.iskeyword(x)
)
