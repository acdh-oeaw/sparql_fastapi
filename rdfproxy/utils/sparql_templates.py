"""SPARQL Query templates for RDFProxy paginations."""

from string import Template


ungrouped_pagination_base_query = Template("""
$query
limit $limit
offset $offset
""")
