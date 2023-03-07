"""
Main file that creates index files of each kind
"""
import sys
sys.path.append('../')
sys.path.append('./')
import os
from indexing.constants import *
import json
from memory import DatabaseStorage
class CreateIndex:
    def __init__(self, database_location = './', database_name = 'database1', collection_name = 'collections1', fields = []) -> None:
        """
        fields holds the set of fields that are to be indexed.
        """
        self.database_location = database_location
        self.fields = fields
        self.collection_name = collection_name
        self.database_name = database_name
        # Check that the collection name and database name are valid, in that the files actually exist
    
    def handle_search(self):
        """
        Load for each word for the values of each field, the entire document in self.storage_index
        Since this is a text search, it cannot handle fields that arent text
        """
        for field in self.fields:
            for _id, doc in self.db.storage['_data'].items():
                if field in doc:
                    # Extract the field, and store.
                    if type(doc[field]) != str:
                        continue
                    data = doc[field].split(" ")
                    for index in data:
                        if index in self.index_storage:
                            self.index_storage[index].append(doc)
                        else:
                            self.index_storage[index] = [doc]
                    if len(data) > 1: # more than 1 word, store entire string also to map to this document
                        if doc[field] in self.index_storage:
                            self.index_storage[doc[field]].append(doc)
                        else:
                            self.index_storage[doc[field]] = [doc]
        # print(self.index_storage)
        self.write_index_file()


    def populate_index(self, index_type):
        """
        First load the collection
        For the relevant collection, create a collection_name_<index_type>.json
        Based on the type, store the required keys with the entire document as the value in a list.
        This way, for that key, all the documents would be returned. 
        This is true for text search may not be true for the others so implementers do think about it.
        """
        self.db = DatabaseStorage(database_location=self.database_location, database_name=self.database_name, collection_name=self.collection_name)
        self.collection_index_name = f'{self.collection_name}_{index_type}_index.json'
        self.create_index_file()
        if index_type == SEARCH:
            self.handle_search()

    def create_index_file(self):
        """
            returns a file descripter if a collection_name was provided.
        """
        if not os.path.exists(f'{self.database_location}/{self.database_name}/{INDEXES}/'):
            os.makedirs(f'{self.database_location}/{self.database_name}/{INDEXES}/')

        file_path = f'{self.database_location}/{self.database_name}/{INDEXES}/{self.collection_index_name}'
        if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
            fd = open(file_path, 'w+')
            self.index_storage = dict()
        else:
            fd = open(file_path, 'a+')
            fd.seek(0)
            self.index_storage = json.load(fd)
    
    def write_index_file(self):
        """
            Write the contents of self.index_storage to the file.
        """
        json.dump(self.index_storage, 
            open(f'{self.database_location}/{self.database_name}/{INDEXES}/{self.collection_index_name}', 'w'),
            indent=4)

