"""
This file will handle hosting the command line tool.
"""
import sys
sys.path.append('./')
sys.path.append('../')
# sys.path.append('../../')
from cmd import Cmd
import re
import os
import ast
import json
from memory import DatabaseStorage
from queries.queryhandler import QueryHandler
from crud.insert import Insert
from crud.update import Update
from crud.delete import Delete
from indexing.text_search import TextSearch
from indexing.create_index import CreateIndex
from interface.constants import *
import string
class CLI(Cmd):
    prompt = '> '
    intro = "Welcome to DocsDB-I1! Type ? to list commands"
    db = None
    database_location = None
    present_directory = None
    collections_list = []
    database_list = []
    
    def getDirectoryList(self, path):
        """
        Get the list of database directories. This returns all directories 
        having JSON files in the current folder. Not the safest mechanism
        as artificial JSON files may also get pulled in, but we will leave 
        it as is for now.
        """
        directoryList = []

        #return nothing if path is a file
        if os.path.isfile(path):
            return []

        #add dir to directorylist if it contains .json files
        if len([f for f in os.listdir(path) if f.endswith('.bson') and 'indexes' not in path])>0:
            directoryList.append(path.split('./')[1])

        for d in os.listdir(path):
            new_path = os.path.join(path, d)
            if os.path.isdir(new_path):
                directoryList += self.getDirectoryList(new_path)

        return directoryList
    def get_databases(self):
        """
        Returns the databases present in the current directory.
        """
        self.present_directory = './'
        self.database_list = self.getDirectoryList(self.present_directory)

    def get_collections(self):
        """
        Returns the collections present under the current directory.
        """
        self.collections_list = []
        for path in os.listdir(self.database_location):
            self.collections_list.append(path.split('.')[0])

    def do_show(self, inp):
        """
        Runs the show command. Inp can be either dbs or collections.
        If not, raise an exception
        """
        if inp == DBS:
            for db in self.database_list:
                print(f">>> {db}")
        elif inp == COLLECTIONS and self.db != None:
            for collection in self.collections_list:
                print(f">>> {collection}")
        else:
            print('Invalid command')

    def do_use(self, inp):
        """
        Changes the current database to the name provided
        """
        if inp == '' or inp == None:
            print('Database Name invalid')
        
        self.db = DatabaseStorage(database_name=inp)
        self.database_location = f'./{inp}/'
        self.get_collections()
        self.db_name = inp

        print('switched to', inp)

    def do_exit(self, inp):
        '''exit the application.'''
        print("Bye")
        return True
    
    def do_db(self, inp):
        '''
        This handles all other standard mongodb inputs
        '''
        self.parser(inp)


    def extract_sel_proj(self, inp):
        """
            Performing paranthesis matching to find the last } matching the first {
            For now assuming [] and () will match on their own.
        """
        if inp[0] != '{':
            return INV_SNTX, ''
        lc, rc = 1,0
        for i,c in enumerate(inp[1:]):
            if c == '{':
                lc += 1
            elif c == '}':
                rc += 1
            if rc == lc:
                # found the match
                r1 = inp[:i+2]
                r2 = inp[i+2:]
                r2 = r2[r2.find('{'):]
                return r1, r2

    def parser(self, inp):
        """
            Parses the input. For each new functionality added, the required class needs to 
            be added to the parser.
        """
        tokens = inp.split('.')
        if len(tokens) < 3 or tokens[1] not in self.collections_list:
            print('Invalid command')
        else:
            # In all other cases, process
            self.collection_name = tokens[1]
            self.db = DatabaseStorage(database_name=self.db_name, collection_name=self.collection_name)
            # Perform {} matching to extract the select and project parts.
            [oprn, *rest] = list(map(str.strip, re.split('\(|\)', tokens[2])))
            if oprn == FIND:
                sel, proj = self.extract_sel_proj(rest[0])
                q = QueryHandler()
                if sel == INV_SNTX:
                    print(INV_SNTX)
                else:
                    q.handle_query(oprn, [json.loads(sel), json.loads(proj)], self.db.storage)
            # CRUD operations only have 1 field {}
            elif oprn == INSERT_ONE:
                payload = json.loads(rest[0])
                ins = Insert()
                ins.insert_one(database=self.db_name, collection_name=self.collection_name, payload=payload)
            elif oprn == INSERT_MANY:
                payloads = json.loads(rest[0])
                ins = Insert()
                ins.insert_many(database=self.db_name, collection_name=self.collection_name, payloads=payloads)
            elif oprn == UPDATE_ONE:
                payload = json.loads(rest[0])
                up = Update()
                up.update_one(database=self.db_name, collection_name=self.collection_name, payload=payload)
            elif oprn == DELETE_ONE:
                payload = json.loads(rest[0])
                delete = Delete()
                delete.delete_one(database=self.db_name, collection_name=self.collection_name, payload=payload)
            elif oprn == SEARCH:
                # Just a single word
                payload = rest[0][1:-1]
                if type(payload)!= str:
                    print("Requires a single word to search")
                else:
                    ts = TextSearch(database_name=self.db_name, collection_name=self.collection_name, word_to_search=payload)
                    ts.search()
            elif oprn == C_INDX:
                # payload is a list of fields
                try:
                    payload = ast.literal_eval(rest[0])
                    index = CreateIndex(database_name=self.db_name, collection_name=self.collection_name, fields=payload)
                    index.populate_index(SEARCH)
                except:
                    print("Require a list of fields")
            elif oprn == LOOKUP:
                lookup_str = rest[0]
                lookup_str.strip("()")
                print(lookup_str)
                lookup_dict = json.loads(lookup_str)
                from_collection = lookup_dict['from']
                local_field = lookup_dict['localField']
                foreign_field = lookup_dict['foreignField']
                as_field = lookup_dict['as']
                type = lookup_dict['type']

                col2 = DatabaseStorage(database_name=self.db_name, collection_name=from_collection)
                col1 = DatabaseStorage(database_name=self.db_name, collection_name=self.collection_name)

                q = QueryHandler()
                q.handle_query('lookup', [col2.storage, local_field, foreign_field, as_field, type], col1.storage)
                
            #  TODO: ADD NEW OPERATIONS HERE.

cli = CLI()
cli.get_databases()
cli.cmdloop()
