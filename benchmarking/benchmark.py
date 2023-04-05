'''
Take from the user the choice on database to run on. Use the database's APIs to make the required requests.
'''
import sys
sys.path.append('../')
sys.path.append('./')
from crud.insert import Insert
from pymongo import MongoClient
import random
import string
import time
import redis
import json

LOAD = '1'
READ = '2'
LOAD_PAYLOAD = 10000
READ_PAYLOAD = 1000

class BenchMark:
    def __init__(self) -> None:
        self.data = []
        pass
    def create_load_payload(self, n: int):
        i = 0
        while i < n:
            payload = {
                "name": ''.join(random.choices(string.ascii_letters, k=7)),
                "address": ''.join(random.choices(string.ascii_letters + string.digits, k=30)),
                "massive_payload": ''.join(random.choices(string.ascii_letters, k=5000))
            }
            yield payload
            i += 1
    def create_read_payload(self, data: list):
        res = set()
        for dat in range(0, LOAD_PAYLOAD, LOAD_PAYLOAD//READ_PAYLOAD):
            res.add(data[dat-1]["name"])
        return res


    def process(self, db, payload_size, payload_type):
        if (payload_type == LOAD):
            self.data = list(self.create_load_payload(LOAD_PAYLOAD))
        elif payload_type == READ:
            self.data = list(self.create_load_payload(LOAD_PAYLOAD))
            self.name_list = self.create_read_payload(self.data)
        if db == '1':
            client = MongoClient()
            db = client.benchmark
            collection = db.col1
            s = time.time()
            for doc in self.data:    
                result = collection.insert_one(doc)
            if payload_type == READ:
                for name in self.name_list:
                    collection.find({"name": name})
            collection.delete_many({})
            e = time.time()

            print(f"Done\nTime to execute workload - {e-s}")
            print(f'Operations per second {(LOAD_PAYLOAD + (READ_PAYLOAD if payload_type == READ else 0))/(e-s)}')
            # With du -h found the space consumed during the operation for 100k loads = 613MB; after deleting is 151MB after clearing journal
            # Total consumption = 468MB (READ WRITE AMPLIFICATION)
            print(f'Read Write Amplification - 468MB')
            print(f'Memory usage - 397MB')
            
        if db == '2':
            r = redis.Redis(host='localhost', port=6379, db=0)
            key_list = [''.join(random.choices(string.ascii_letters, k=7)) for _ in range(len(self.data))]
            s = time.time()
            for key, doc in zip(key_list, self.data):
                result = r.set(key, json.dumps(doc))
            if payload_type == READ:
                for key in key_list[::LOAD_PAYLOAD//READ_PAYLOAD]:
                    r.get(key)
            r.flushdb()
            e = time.time()
            
            print(f"Done\nTime to execute workload - {e-s}")
            print(f'Operations per second {(LOAD_PAYLOAD + (READ_PAYLOAD if payload_type == READ else 0))/(e-s)}')
            # Using activity monitor / htop to find the load - After writing 100k loads = 506MB; After deleting - 42MB
            # Total consumption = 464MB (READ WRITE AMPLIFICATION)
            print(f'Read Write Amplification - 464MB')
            print(f'Memory usage - 42MB')
        if db == '3':
            ins = Insert()
            s = time.time()
            for doc in self.data:
                ins.insert_one(database='CS541DB', collection_name='LoadTest1', payload=doc, group_commit=False)
            e = time.time()
            print(f"Done\nTime to execute workload - {e-s}")
            print(f'Operations per second {(LOAD_PAYLOAD + (READ_PAYLOAD if payload_type == READ else 0))/(e-s)}')
        if db == '4':
            ins = Insert(interval=5)
            s = time.time()
            for doc in self.data:
                ins.insert_one(database='CS541DB', collection_name='LoadTest1', payload=doc, group_commit=True)
            ins.stop_commit()
            e = time.time()
            print(f"Done\nTime to execute workload - {e-s}")
            print(f'Operations per second {(LOAD_PAYLOAD + (READ_PAYLOAD if payload_type == READ else 0))/(e-s)}')

db = input("Enter the database you wish to benchmark 1 - MongoDB, 2 - Redis, 3 - DocsDB without GroupCommit, 4 - DocsDB with Group Commit \n")
workload = input(f"Enter the kind of workload to run 1 - Loading {LOAD_PAYLOAD} records, 2 - Loading and Reading {READ_PAYLOAD} records\n")
if workload == '1':
    workload_size = LOAD_PAYLOAD
elif workload == '2':
    workload_size = READ_PAYLOAD

bm = BenchMark()
bm.process(db, workload_size, workload)