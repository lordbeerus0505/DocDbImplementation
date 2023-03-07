import sys
sys.path.append('../')
from memory import MemoryStorage

mem_store = MemoryStorage()
dbs = mem_store.read()

print(dbs)

dbs["database1"] = {}
dbs["database2"] = {}

print(dbs)