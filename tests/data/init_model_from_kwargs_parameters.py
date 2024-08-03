"""Model and kwargs data mappings for testing."""

from tests.data.models import ComplexModel, NestedModel, SimpleModel


init_model_from_kwargs_parameters = [
    (SimpleModel, [{"x": 1}, {"x": 1, "y": 2}]),
    (
        NestedModel,
        [
            {"x": 1, "a": "a value"},
            {"x": 1, "y": 2, "a": "a value"},
        ],
    ),
    (
        ComplexModel,
        [
            {"p": "p value", "a": "a value", "x": 1},
            {"p": "p value", "a": "a value", "x": 1, "y": 2},
        ],
    ),
]
