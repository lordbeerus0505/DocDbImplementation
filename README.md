# DocDbImplementation
Implementing a document oriented database in Python


### CLI Commands
`python ./interface/cli_runner.py`
#### Basic CRUD
```
show dbs
use CS541DB
show collections
db.Projects.insertOne({"_id":"demo_index12345", "title": "demo Project", "members":["member1", "member2"], "code": "SampleDB1"})
db.Projects.find({"title": {"eq": "demo Project"}}, {"code":1, "members":1})
db.Projects.updateOne({"_id":"demo_index12345", "title": "demo Project", "members":["member1", "member2", "member3"], "code": "SampleDB1"})
db.Projects.find({"title": {"eq": "demo Project"}}, {"code":1, "members":1})
db.Projects.deleteOne({"_id": "demo_index12345"})
```
#### Text Search
```
use test/CS541DB
db.mock_collections.createIndex(["last_name", "first_name"])
db.mock_collections.search("Echallier")
```

#### Containers
First create the dockerfile.
To execute - 
```
If image doesnt already exist -
docker build -it docsdbi1 .
./docker_run.sh
```


To clean up - 
```
docker ps -a | grep <container name>
docker stop <containerid>
docker rm <containerid>
docker rmi <image name>

```

#### Indexes 
We have implemented 2 classes of indexing - B-tree and RTree(Multi-attribute indexing)

They have not been included in the CLI yet. Their usage is defined with comments and examples in test/indexing folder.

Functionalities implemented using index trees:
1. Range search using BTrees
2. 2D and 3D range search (Finding points in a rectangular bounding box) using RTrees
3. Finding nearest neighbour in 2D and 3D space.

Other features:'
1. The indexes are persistent indexes - They are stored in the disk as ".idx" files.

Future work: Include indexing in CLI.
