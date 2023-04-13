#! /bin/bash
# docker build -t docsdbi1 .
docker run -dit -p 8080:80 --name docsdb docsdbi1
DB_HASH=`docker ps -aqf 'name=docsdb'`
docker exec -it $DB_HASH /bin/python3 interface/cli_runner.py