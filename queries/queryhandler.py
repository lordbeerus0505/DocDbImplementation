"""
This file creates a handler used to pass the json to 
each type of handler based on the query.
"""
import json
from .find import FindQuery
from .lookup import LookUp

class QueryHandler:
    query_request = None
    query_response = None
    def __init__(self) -> None:
        pass

    def handle_query(self, query_type: str, query: list, collection: json, return_as_json:bool = False):
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
            # print (metadata)
            for count, q in enumerate(query):
                if count < len(query) - 1:
                    [(oprn,val)] = q.items()

                    if count == len(query) - 2:
                        oprn_query = [val,query[-1]]
                    else:
                        oprn_query = [val,{}]
                    
                    if oprn == 'find':
                        # print ("Data before find -", chosen_data)
                        fq = FindQuery()
                        fq.find_handler(chosen_data, *oprn_query)
                        chosen_data = {"_metadata":metadata, "_data" : fq.select_results}
                        result = fq.project_results
                        # print ("Data after find -", chosen_data)

                    elif oprn == 'group':
                        # print ("Data before groupby - ", chosen_data)
                        gbq = GroupByQuery()
                        gbq.groupby_handler(chosen_data, *oprn_query)
                        chosen_data = {"_metadata":metadata, "_data" : gbq.group_results}
                        result = gbq.project_results
                        # print ("Data after groupby - ", chosen_data)
            
            print (result)            
            pass
