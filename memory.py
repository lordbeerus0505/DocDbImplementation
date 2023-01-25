import json
import os

class ListOfDatabases:
    """
    Need a file that will store the name of all databases. 
    The path for this file will be fixed in later stages to where the library is installed,
    but for now, let it reside in the directory of execution.
    This way if a new database is being created, it should have a different name
    """
    databases = []
    descripter = None
    def __init__(self, path_to_file= 'database_list.txt'):
        f = open(path_to_file, "a+")
        f.seek(0) # move pointer to the start
        self.databases = f.read().splitlines()
        self.descripter = f
    
    def get_database_names(self):
        return self.databases
    
    def add_database_names(self, new_database_name):
        self.descripter.write(f'{new_database_name}\n')
    
    def close_database_names(self):
        self.descripter.close()

class DatabaseStorage:
    """
    Store each table in a JSON file. Provide the path to the database where the files are to be stored.
    The database is a directory and each file is a collection (table in relational models).
    Each collection will have one or more documents (a JSON structure)
    """
    database = ''
    storage = None
    collection_name = None
    def create_directory(self,directory_path):
        """
        Provide an absolute path for the directory that is to be created.
        returns if success or failure
        """
        base_dir = os.path.dirname(directory_path)
        if not os.path.exists(base_dir):
            try:
                os.makedirs(base_dir)
            except:
                return False
        self.database = directory_path
        return True

    def __init__(self, database_location, database_name, collection_name=None):
        """
        Database_location provides path of the directory where the database is to be stored
        Collection_name is the name of the collection to create if any. Its possible that
        we want to just create a database without creating a collection
        """
        list_of_db = ListOfDatabases()
        if database_name in list_of_db.get_database_names():
            # Need to validate that the directory exists in the location specified
            if not os.path.exists(database_location):
                raise Exception("Database already exists as a different location, \
                    choose a different name or change the location")

        if self.create_directory(f'{database_location}/{database_name}/'):
            list_of_db.add_database_names(database_name)
            # Now if we need to create/update/read a collection, we shall do so.
            self.create_file(collection_name)
        else:
            raise Exception("Could not create database at the location provided.")
       
    def create_file(self, collection_name):
        """
            returns a file descripter if a collection_name was provided.
        """
        def empty_file(name):
            import datetime
            payload = {
                "_metadata" : {
                    "collection_name" : name,
                    "creation_time" : datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                }, 
                "_data" : []
            }
            return payload
        if collection_name:
            if not os.path.exists(f'{self.database}/{collection_name}.json')\
                or os.stat(f'{self.database}/{collection_name}.json').st_size == 0:
                import datetime
                fd = open(f'{self.database}/{collection_name}.json', "w+")
                self.storage = json.loads('''{
                    "_metadata": {"collection_name": "%s", "creation_time": "%s"},
                    "_data" : []
                }'''%(collection_name, datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
                # import pdb; pdb.set_trace()
            else:
                fd = open(f'{self.database}/{collection_name}.json', "a+")
                fd.seek(0)
                self.storage = json.load(fd)
            self.collection_name = collection_name

    def write_file(self):
        """
            Write the contents of self.storage to the file.
        """
        json.dump(self.storage, 
            open(f'{self.database}/{self.collection_name}.json', 'w'),
            indent=4)