"""
This file will handle hosting the command line tool.
"""
import sys
sys.path.append('./')
sys.path.append('../')
sys.path.append('../../')
from cmd import Cmd
import os
from memory import DatabaseStorage
class CLI(Cmd):
    prompt = '> '
    intro = "Welcome to DocsDB-I1! Type ? to list commands"
    DBS = 'dbs'
    COLLECTIONS = 'collections'
    USE = 'use'
    db = None
    database_location = None
    present_directory = None
    collections_list = []
    database_list = []
    
    def getDirectoryList(self, path):
        directoryList = []

        #return nothing if path is a file
        if os.path.isfile(path):
            return []

        #add dir to directorylist if it contains .txt files
        if len([f for f in os.listdir(path) if f.endswith('.json')])>0:
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
        if inp == self.DBS:
            for db in self.database_list:
                print(f">>> {db}")
        elif inp == self.COLLECTIONS and self.db != None:
            for collection in self.collections_list:
                print(f">>> {collection}")
        else:
            print('Invalid command')

    def do_use(self, inp):
        """
        Changes the current database to the name provided
        """
        if inp == '':
            print('Database Name invalid')
        
        self.db = DatabaseStorage(database_name=inp)
        self.database_location = f'./{inp}/'
        self.get_collections()

        print('switched to', inp)

    def do_exit(self, inp):
        '''exit the application.'''
        print("Bye")
        return True
    
    def default(self, line):
        '''
        This handles all other standard mongodb inputs
        '''
        print('hello')

cli = CLI()
cli.get_databases()
cli.cmdloop()
