"""
File for GROUPBY query
"""
class GroupByQuery:
    project_results = None
    group_results = None

    def __init__(self) -> None:
        self.group_results = {}
        self.project_results = []

    def grouping(self, field, data):
        for ele in data:
            if field in ele:
                if ele[field] in self.group_results:
                    self.group_results[ele[field]].append(ele)
                else:
                    self.group_results[ele[field]] = [ele]
    
    def group_count(self, field, group_list):
        unique_set = set()

        for ele in group_list:
            if field in ele:
                unique_set.add(ele[field])

        return len(unique_set)
    
    def group_max(self, field, group_list):
        unique_set = set()
        for ele in group_list:
            if field in ele:
                unique_set.add(ele[field])
        
        return max(unique_set)

    def group_min(self, field, group_list):
        unique_set = set()
        for ele in group_list:
            if field in ele:
                unique_set.add(ele[field])
        
        return min(unique_set)
    
    def group_avg(self, field, group_list):
        unique_list = []
        for ele in group_list:
            if field in ele:
                if type(ele[field]) != str:
                    unique_list.append(ele[field])
        
        if len(unique_list) == 0:
            return "N/A"

        return (sum(unique_list)/len(unique_list))
        
    def groupby_handler(self, collection, group_query, project_query):
        """
        Receives a collection and a query.
        Parse the query and groupby the fields mentioned
        The first <k,v> pair is the select, the second is the project
        """
        # First select the results
        # TODO: Support more than 1 query condition. Currently it handles just one. Handle AND, OR
        data = collection['_data']
        # print (data)
        for field, constraints in group_query.items():
            if field == "_id":
                self.grouping(constraints, data)
            else:
                raise Exception("Invalid constraint, check query")
        
        for group, group_list in self.group_results.items():
            project_dict = {}
            for field, value in project_query.items():
                if value == 1:
                    if len(group_list) > 0:
                        if field in group_list[0]:
                            project_dict[field] = group_list[0][field]
                        else:
                            project_dict[field] = "null"
                
                elif value == 2:
                    project_dict["count("+field+")"] = self.group_count(field, group_list)
                
                elif value == 3:
                    project_dict["max("+field+")"] = self.group_max(field, group_list)
                
                elif value == 4:
                    project_dict["min("+field+")"] = self.group_min(field, group_list)
                
                elif value == 5:
                    project_dict["avg("+field+")"] = self.group_avg(field, group_list)
            
            self.project_results.append(project_dict)
        
        return
