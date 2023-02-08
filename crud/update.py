"""
File handles updateOne operation where we search for a document in a collection using PK (_id). 
Later designs should also support a filter for search and other such keys as well as updateMany.
Also, just like MongoDB, an update operation performs a replace of the document, not just updates 
to specific fields.
"""
from memory import DatabaseStorage

class Update:
    def __init__(self) -> None:
        pass

    def update_one(self, database, collection_name, payload, database_location='./'):
        if '_id' in payload:
            db = DatabaseStorage(database_location=database_location, database_name= database, collection_name= collection_name)
            # TODO: This forces the creation of a database and collection if they dont exist. 
            # Delete those creations in the future.
            if db.storage['_data'] == []:
                raise Exception("Empty Collection")
            for i,entry in enumerate(db.storage['_data']):
                if payload['_id'] == entry['_id']:
                    db.storage['_data'].pop(i)
                    db.storage['_data'].append(payload)
                    db.write_file()
                    return "OK"

            raise Exception("Key not found")

        else:
            # Check that the filters match the condition. TODO
            return "OK"
