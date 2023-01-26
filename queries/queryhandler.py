"""
This file creates a handler used to pass the json to 
each type of handler based on the query.
"""
import json
from .find import FindQuery
class QueryHandler:
    query_request = None
    query_response = None
    def __init__(self) -> None:
        pass

    def handle_query(self, query_type: str, query: list, collection: json):
        """
        A possible input:
        db.collection.find(name eq tom)

        """
        if query_type == 'find':
            fq = FindQuery()
            fq.find_handler(collection, *query)
            print(fq.project_results)
            pass
