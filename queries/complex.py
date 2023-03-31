import json
from .find import FindQuery
from .lookup import LookUp
from .groupby import GroupByQuery

"""
File for Complex query
"""
class ComplexQuery:

    def complex_handler(self, chosen_data, query):
        for count, q in enumerate(query):
            if count < len(query) - 1:
                [(oprn,val)] = q.items()

                if count == len(query) - 2:
                    oprn_query = [val,query[-1]]
                else:
                    oprn_query = [val,{}]
                
                if oprn == 'find':
                    fq = FindQuery()
                    fq.find_handler(chosen_data, *oprn_query)
                    chosen_data = {"_metadata":metadata, "_data" : fq.select_results}
                    result = fq.project_results

                elif oprn == 'group':
                    gbq = GroupByQuery()
                    gbq.groupby_handler(chosen_data, *oprn_query)
                    chosen_data = {"_metadata":metadata, "_data" : gbq.group_results}
                    result = gbq.project_results
        
        return result