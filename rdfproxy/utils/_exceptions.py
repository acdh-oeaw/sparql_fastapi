"""Custom exceptions for RDFProxy."""


class UndefinedBindingException(KeyError):
    """Exception for indicating that a requested key could not be retrieved from a SPARQL binding mapping."""


class InterdependentParametersException(Exception):
    """Exceptiono for indicating that two or more parameters are interdependent."""
