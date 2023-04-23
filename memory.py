import json
import os
import bson

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
        if new_database_name not in self.databases:
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
    catalog = None
    catalog_name = None
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

    def __init__(self, database_location='./', database_name='database1', collection_name=None, index_keys = None, catalog = None):
        """
        Database_location provides path of the directory where the database is to be stored
        Collection_name is the name of the collection to create if any. Its possible that
        we want to just create a database without creating a collection. Option is given to create
        index for a collection on creation.
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
            # Create index for the collection
            self.create_catalog(collection_name+"_catalog")

        else:
            raise Exception("Could not create database at the location provided.")
       
    def create_file(self, collection_name):
        """
            returns a file descripter if a collection_name was provided.
        """
        if collection_name:
            if not os.path.exists(f'{self.database}/{collection_name}.bson')\
                or os.stat(f'{self.database}/{collection_name}.bson').st_size == 0:
                import datetime
                fd = open(f'{self.database}/{collection_name}.bson', "wb")
                fd.close() # just create a file so another request to same method doesnt end up here.
                self.storage = json.loads('''{
                    "_metadata": {"collection_name": "%s", "creation_time": "%s"},
                    "_data" : {}
                }'''%(collection_name, datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
            else:

                with open(f'{self.database}/{collection_name}.bson', "rb") as f:
                    self.storage = bson.BSON.decode(f.read())
            self.collection_name = collection_name
    
    def create_catalog(self, catalog_name):
        """
            returns a file descriptor for the catalog with the name provided
        """
        if catalog_name:
            if not os.path.exists(f'{self.database}/{catalog_name}.json')\
                or os.stat(f'{self.database}/{catalog_name}.json').st_size == 0:
                import datetime
                fd = open(f'{self.database}/{catalog_name}.json', "w")
                fd.close() # just create a file so another request to same method doesnt end up here.
                self.catalog = json.loads('''{
                    "_metadata": {"catalog_name": "%s", "creation_time": "%s"},
                    "_data" : {}
                }'''%(catalog_name, datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
            else:

                with open(f'{self.database}/{catalog_name}.json', "r") as f:
                    self.catalog = json.load(f)
            self.catalog_name = catalog_name


    def write_file(self):
        """
            Write the contents of self.storage to the file.
        """

        data = bson.BSON.encode(self.storage)
        with open(f'{self.database}/{self.collection_name}.bson', 'wb') as f:
            f.write(data)
        
        data = self.catalog
        with open(f"{self.database}/{self.catalog_name}.json", "w") as f:
            f.write(json.dumps(data, indent=4))
