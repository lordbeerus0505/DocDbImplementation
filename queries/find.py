"""
This file will handle find queries.
"""

class FindQuery:
    select_results = None
    project_results = None
    def __init__(self) -> None:
        self.select_results = []
        self.project_results = []
        
    def handle_eq(self,data, field, val):
        for ele in data:
            if field in ele and ele[field] == val:
                self.select_results.append(ele)


    def handle_gt(self, data, field, val):
        for ele in data:
            if field in ele and  ele[field] > val:
                self.select_results.append(ele)

    def handle_lt(self, data, field, val):
        for ele in data:
            if field in ele and ele[field] < val:
                self.select_results.append(ele)
    
    def handle_ne(self, data, field, val):
        for ele in data:
            if field in ele and ele[field] != val:
                self.select_results.append(ele)
        

    def find_handler(self, collection, select_query, project_query):
        """
        Receives a collection and a query.
        Parse the query and search for it in the collection.
        The first <k,v> pair is the select, the second is the project

        """
        # First select the results
        # TODO: Support more than 1 query condition. Currently it handles just one. Handle AND, OR
        data = collection['_data']
        for field, constraints in select_query.items():
            print(field, constraints)
            # import pdb; pdb.set_trace()
            # TODO: index to prevent linear search
            for k,v in constraints.items():
                if k == 'eq':
                    self.handle_eq(data, field, v)
                elif k == 'gt':
                    self.handle_gt(data, field, v)
                elif k == 'lt':
                    self.handle_lt(data, field, v)
                elif k == 'ne':
                    self.handle_ne(data, field, v)
                elif k == 'geq':
                    self.handle_gt(data, field, v)
                    self.handle_eq(data, field, v)
                elif k == 'leq':
                    self.handle_lt(data, field, v)
                    self.handle_eq(data, field, v)
                else:
                    # No Match
                    raise Exception("Invalid constraint, check query")

        
        # Now to project the results
        for ele in self.select_results:
            tupl = {}
            for field, present in project_query.items():
                if present == 1 and field in ele:
                    tupl[field] = ele[field]
            self.project_results.append(tupl)
        
