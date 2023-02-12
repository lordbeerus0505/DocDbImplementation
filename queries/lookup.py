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
            self.equality_match_handler(collection, *args)
        else:
            raise NotImplementedError

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
        

        