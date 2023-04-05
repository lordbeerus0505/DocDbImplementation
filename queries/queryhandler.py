"""
This file creates a handler used to pass the json to 
each type of handler based on the query.
"""
import json
from .find import FindQuery
from .lookup import LookUp
from .groupby import GroupByQuery
from .complex import ComplexQuery

class QueryHandler:
    query_request = None
    query_response = None
    def __init__(self) -> None:
        pass

    def handle_query(self, query_type: str, query: list, collection: json, return_as_json:bool = False, op = None):
        """
        A possible input:
        db.collection.find(name eq tom)

        """
        if query_type == 'find':
            fq = FindQuery()
            fq.find_handler(collection, *query)
            print(json.dumps(fq.project_results, indent=4))
            if return_as_json:
                return fq.project_results
        elif query_type == 'lookup':
            lq = LookUp(collection, *query)
            lq.lookup_handler(collection)
            print(json.dumps(lq.lookup_results, indent=4))        
        elif query_type == 'group':
            gbq = GroupByQuery()
            gbq.groupby_handler(collection, *query)
            print(gbq.project_results)
            pass
        
        elif query_type == 'complex':
            chosen_data = collection
            metadata = collection["_metadata"]
            cq = ComplexQuery()
            result = cq.complex_handler(chosen_data, *query)
            print (result)            
            pass
