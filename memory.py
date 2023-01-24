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
    descriptor = None
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
            raise Exception("Database already exists, choose a different name")
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
        if collection_name:
            fd = open(f'{self.database}/{collection_name}.json', 'a+')
            fd.seek(0)
            self.descriptor = fd

    
    # def touch(fname, times=None, create_dirs=False):
    #     if create_dirs:
    #         base_dir = os.path.dirname(fname)
    #         if not os.path.exists(base_dir):
    #             os.makedirs(base_dir)
    #     with open(fname, 'a'):
    #         os.utime(fname, times)

    # def close(self):
    #     self._handle.close()

    # def read(self):
    #     # Get the file size
    #     self._handle.seek(0, os.SEEK_END)
    #     size = self._handle.tell()

    #     if not size:
    #         # File is empty
    #         return None
    #     else:
    #         self._handle.seek(0)
    #         return json.load(self._handle)

    # def write(self, data):
    #     self._handle.seek(0)
    #     serialized = json.dumps(data, **self.kwargs)
    #     self._handle.write(serialized)
    #     self._handle.flush()
    #     self._handle.truncate()