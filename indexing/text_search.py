"""
Sets up a text searching mechanism. Takes from the user a request to create a text search index
and adds a text searching json file.
"""
import json
import bson
from indexing.constants import *
class TextSearch:
    def __init__(self, database_location = './', database_name = 'database1', collection_name = 'collection1', word_to_search = None) -> None:
        self.database_location = database_location
        self.database_name = database_name
        self.collection_name = collection_name
        self.word_to_search = word_to_search

    def preprocess(self):
        self.collection_index_name = f'{self.collection_name}_search_index.bson'
        file_path = f'{self.database_location}/{self.database_name}/{INDEXES}/{self.collection_index_name}'
        with open(file_path, "rb") as f:
            self.index_storage = bson.BSON.decode(f.read())

    def search(self):
        self.preprocess()
        documents = self.index_storage[self.word_to_search]
        print(json.dumps(documents, indent=4))
