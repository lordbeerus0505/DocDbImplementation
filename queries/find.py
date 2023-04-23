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
        for _, ele in data.items():
            if field in ele and ele[field] == val:
                self.select_results.append(ele)


    def handle_gt(self, data, field, val):
        for _, ele in data.items():
            if field in ele and  ele[field] > val:
                self.select_results.append(ele)

    def handle_lt(self, data, field, val):
        for _, ele in data.items():
            if field in ele and ele[field] < val:
                self.select_results.append(ele)
    
    def handle_ne(self, data, field, val):
        for _, ele in data.items():
            if field in ele and ele[field] != val:
                self.select_results.append(ele)
    
    def operate(self, op, X, Y):
        if op == 'eq':
            return X == Y
        elif op == 'ne':
            return X != Y
        elif op == 'gt':
            return X > Y
        elif op == 'lt':
            return X < Y
        elif op == 'geq':
            return X >= Y
        elif op == 'leq':
            return X <= Y
        else:
            raise Exception("Invalid operator")
    
    def handle_and(self, data, field, constraints):
        """
        Handles the AND operator. To do so, performing an intersection of all the results across constraints.
        Since these are dictionary entries, cannot perform an intersection directly. Instead, we must store
        just the _id as a key and perform intersection on that.
        """
        res = set()
        data_map = dict()
        for _, ele in data.items():
            res.add(ele["_id"])
            data_map[ele["_id"]] = ele
        # Now intersect res with the results of each
        
        
        for constraint in constraints:
            row = set()
            for k,v in constraint.items(): # O(1) as only 1 KV pair
                for _, ele in data.items():
                    if field in ele and self.operate(k, ele[field], v):
                        row.add(ele['_id'])

            res = res.intersection(row)

        for element in res:
            self.select_results.append(data_map[element])

    def handle_or(self, data, field, constraints):
        """
            Handles the OR operator. Performs a union of all the results by simply using a set
            This helps prevent having duplicates
        """
        res = set()
        data_map = dict() # populating the dictionary only for matching rows as its pointless otherwise 

        for constraint in constraints:
            row = set()
            for k,v in constraint.items(): # O(1) as only 1 KV pair
                for _, ele in data.items():
                    if field in ele and self.operate(k, ele[field], v):
                        row.add(ele['_id'])
                        data_map[ele["_id"]] = ele

            res = res.union(row)
        
        for element in res:
            self.select_results.append(data_map[element])

            
    def short_circuit(self, collection, catalog, select_query):
        """
            If query has > < symbols, check with min and max for the attribute
            If outside it will reject
        """
        for field, constraints in select_query.items():
            for attr,val in constraints.items():
                if attr in catalog:
                    if attr in ['gt', 'geq'] and catalog[attr]["max"] < val:
                        return False
                    elif attr in ['lt', 'leq'] and catalog[attr]["min"] > val:
                        return False


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
                elif k == 'AND' :
                    # The value should be a list, else raise exception
                    if not isinstance(v, list):
                        raise Exception("Expected a list")
                    self.handle_and(data, field, v)
                elif k == 'OR':
                    if not isinstance(v, list):
                        raise Exception("Expected a list")
                    self.handle_or(data, field, v)
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
        
