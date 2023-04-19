"""
File handles the storage of a collection into chunks
"""

import sys
import os
from chunks.constants import *
import json
import re
import time
sys.path.append('./')
sys.path.append('../')

class Chunkify:

    def __init__(self, database_name = 'database1', collection_name = 'collections1') -> None:

        self.collection_name = collection_name
        self.database_name = database_name
        self.collection_path = self.database_name + "/" + self.collection_name

        if not self.checkIfMetaDataFileExists():
            self.createMetaFile()
            
    def createMetaFile(self):

        meta_file = {}
        meta_file["collection_name"] = self.collection_name
        meta_file["database_name"] = self.database_name
        meta_file["File_List_In_Sequence"] = []
        meta_file["TS_Last_Modified"] = []
        meta_file["Space_left_in_file"] = []
        meta_file["Indexed"] = []

        json_meta = json.dumps(meta_file, indent = 4)
        meta_file_path = f'{self.collection_path}/{META_DATA_FILE_NAME}'

        with open(meta_file_path, "w") as json_meta_file:
            json_meta_file.write(json_meta)
        
        return

            # self.verifyer()
        # self.to_be_indexed()
        # print (self.free_files_search(4097))
        # self.modify_file("collection_test_0.json")
 # print("I am being called")
           
    def checkIfMetaDataFileExists(self):

        return os.path.isfile(self.collection_path + "/" + META_DATA_FILE_NAME)
    
    def verifyer(self):

        meta_file = open(f'{self.collection_path}/{META_DATA_FILE_NAME}')
        metadata = json.load(meta_file)
        # print ("IN VERIFYER")
        meta_file.close()        
        total_files = 0

        for f in os.listdir(self.collection_path):
            if os.path.isfile(f'{self.collection_path}/{f}'):
                if f.startswith(self.collection_name):
                    total_files = total_files + 1
                    # print (os.path.getsize(f'{self.collection_path}/{f}')," - file size of ", f)
                    if os.path.getsize(f'{self.collection_path}/{f}') > CHUNK_SIZE:
                        print (f," file in ",self.collection_name," seems messed up in terms of size. Please check")
                        return 0
        
        if len(metadata['Indexed']) != total_files:
            print (self.collection_name,"in",self.database_name,"database seems messed up. Please check")
            return 0
        
        if len(metadata['Space_left_in_file']) != total_files:
            print (self.collection_name,"in",self.database_name,"database seems messed up. Please check")
            return 0
        
        if len(metadata['File_List_In_Sequence']) != total_files:
            print (self.collection_name,"in",self.database_name,"database seems messed up. Please check")
            return 0
        
        return 1
    
    def free_files_search(self, size):

        meta_file = open(f'{self.collection_path}/{META_DATA_FILE_NAME}')
        metadata = json.load(meta_file)
        meta_file.close()
        free_space = metadata['Space_left_in_file']
        free_index = -1
        free_file_name = ""

        for i in range(len(free_space)-1, -1, -1):
            if free_space[i] > size:
                free_index = i
                break

        if free_index == -1:
            # print("Looking for new file")
            free_file_name = self.add_file()
        else:
            free_file_name = self.collection_name + "_" + str(free_index) + ".json"

        return free_file_name
    
    def modify_file(self, file_name):

        meta_file = open(f'{self.collection_path}/{META_DATA_FILE_NAME}')
        metadata = json.load(meta_file)
        meta_file.close()
        match = re.search(r"_(?!.*_)(\w+)\.json$", file_name)

        if match:
            file_no = int(match.group(1))
            metadata["Indexed"][file_no] = 0
            metadata["Space_left_in_file"][file_no] = CHUNK_SIZE - os.path.getsize(f'{self.collection_path}/{file_name}')
            metadata["TS_Last_Modified"][file_no] = time.ctime(os.path.getmtime(f'{self.collection_path}/{file_name}'))            

            if metadata["Space_left_in_file"][file_no] < 0:
                metadata["Space_left_in_file"][file_no] = 0

            with open(f'{self.collection_path}/{META_DATA_FILE_NAME}', 'w') as f:
                json.dump(metadata, f, indent = 4)

        else:
            print("Invalid file name, please check")
            return

        pass
    
    def add_file(self):

        meta_file = open(f'{self.collection_path}/{META_DATA_FILE_NAME}')
        metadata_old = json.load(meta_file)
        meta_file.close()
        metadata = metadata_old
        new_file_name = ""

        if not self.verifyer():
            new_file_name = self.collection_name + "_" + str(len(metadata['File_List_In_Sequence'])) + ".json"
            metadata['File_List_In_Sequence'].append(new_file_name)
            metadata['Space_left_in_file'].append(CHUNK_SIZE)
            metadata['Indexed'].append(0)
            metadata['TS_Last_Modified'].append("NOW")

            with open(f'{self.collection_path}/{new_file_name}','w'):
                pass

            with open(f'{self.collection_path}/{META_DATA_FILE_NAME}', 'w') as f:
                json.dump(metadata, f, indent = 4)

        return new_file_name
    
    def to_be_indexed(self):

        meta_file = open(f'{self.collection_path}/{META_DATA_FILE_NAME}')
        metadata = json.load(meta_file)
        meta_file.close()
        index_list = metadata['Indexed']
        to_be_indexed_list = [f"{self.collection_name}_{i}.json" for i, x in enumerate(index_list) if x == 0]

        return to_be_indexed_list
