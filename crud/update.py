"""
File handles updateOne operation where we search for a document in a collection using PK (_id). 
Later designs should also support a filter for search and other such keys as well as updateMany.
Also, just like MongoDB, an update operation performs a replace of the document, not just updates 
to specific fields.
"""
import threading
from memory import DatabaseStorage

class Update:
    def __init__(self, interval: int = 0) -> None:
        self.db = None
        self.interval = interval
        self.start_commit = False

    def writeToDisk(self):
        self.db.write_file()
    
    def stop_commit(self):
        self.runtime.cancel()
        self.start_commit = False
        self.db.write_file()
        print("Commit complete")

    def commit(self,):
        self.runtime = threading.Timer(self.interval, self.commit)
        self.runtime.start()
        print("Committing")
        self.writeToDisk()

    def update_one(self, database, collection_name, payload, database_location='./', group_commit: bool = False):
        if not group_commit:
            self.db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
        elif (group_commit and not self.start_commit):
            # creates the database and the collection if they dont exist as well.
            self.db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
            self.start_commit = True
            self.commit()
        if '_id' in payload:
            self.db = DatabaseStorage(database_location=database_location, database_name= database, collection_name= collection_name)
            # TODO: This forces the creation of a database and collection if they dont exist. 
            # Delete those creations in the future.
            if self.db.storage['_data'] == []:
                if self.start_commit:
                    self.stop_commit()
                raise Exception("Empty Collection")

            if payload['_id'] in self.db.storage['_data']:
                self.db.storage['_data'][payload['_id']] = payload
                if not self.start_commit:
                    self.db.write_file()
                return "OK"
            if self.start_commit:
                self.stop_commit()
            raise Exception("Key not found")

        else:
            # Check that the filters match the condition. TODO
            return "OK"
