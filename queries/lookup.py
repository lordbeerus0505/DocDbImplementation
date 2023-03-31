""" File to define lookup pipeline stage for queries"""

class LookUp:
    """ 
    The lookup is similar to the lookup in MongoDB. It uses Left Outer Join on the teo collections 
    """
    def __init__(self, collection, from_collection, localField, foreignField, as_field, type="equality_match") -> None:
        """ 
        The fields are similar to MongoDB and serve the same purpose 
        """
        self.collection = collection
        self.from_collection = from_collection
        self.localField = localField
        self.foreignField = foreignField
        self.as_field = as_field
        self.lookup_type = type

        self.lookup_results = None

    def lookup_handler(self, collection):
        """
        This function handles the lookup query.
        """
        args = [self.from_collection, self.localField, self.foreignField, self.as_field]
        if self.lookup_type == "equality_match":
            self.match_handler(collection, self.__eq__, *args)
        elif self.lookup_type == "less_than":
            self.match_handler(collection, self.__lt__, *args)
        elif self.lookup_type == "less_than_or_equal":
            self.match_handler(collection, self.__le__, *args)
        elif self.lookup_type == "greater_than":
            self.match_handler(collection, self.__gt__, *args)
        elif self.lookup_type == "greater_than_or_equal":
            self.match_handler(collection, self.__ge__, *args)
        elif self.lookup_type == "not_equal":
            self.match_handler(collection, self.__ne__, *args)

    def equality_match_handler(self, collection, from_collection, localField, foreignField, as_field):
        """
        This function handles the lookup query with equality match.
        """
        input_data = collection['_data']
        from_data = from_collection['_data']
        as_field_list = as_field.split(".")
        for document in input_data:
            for from_document in from_data:
                if document[localField] == from_document[foreignField]:
                    for key in as_field_list[:-1]:
                        if key in document:
                            document = document[key]
                        else:
                            document[key] = None
                            document = document[key]
                    document[as_field_list[-1]] = from_document
        print(input_data)
        self.lookup_results = input_data

    """
    Function for not just Natural join but for Theta Join
    """
    def match_handler(self, collection, op, from_collection, localField, foreignField, as_field):
        """
        This function handles the lookup query with inequality match.
        """
        input_data = collection['_data']
        from_data = from_collection['_data']
        as_field_list = as_field.split(".")
        for i_key, i_value in input_data.items():
            for f_key, f_value in from_data.items():
                if op(i_value[localField], f_value[foreignField]):
                    for key in as_field_list[:-1]:
                        if key in i_value:
                            i_value = i_value[key]
                        else:
                            i_value[key] = {}
                            i_value = i_value[key]
                    i_value[as_field_list[-1]] = f_value
        print(input_data)
        self.lookup_results = input_data
        
    # Define different operations here
    def __eq__(self, a, b):
        return a == b
    def __lt__(self, a, b):
        return a < b
    def __le__(self, a, b):
        return a <= b
    def __gt__(self, a, b):
        return a > b
    def __ge__(self, a, b):
        return a >= b
    def __ne__(self, a, b):
        return a != b
        