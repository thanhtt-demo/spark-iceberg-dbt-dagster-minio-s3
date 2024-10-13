# install hive to connect db
docker run -d -p 10010:10000 -p 10012:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:4.0.1
docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'

