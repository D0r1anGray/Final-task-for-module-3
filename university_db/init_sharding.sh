#!/bin/bash
# Run this ONCE after `docker-compose up -d` to configure sharding.
# Wait ~10 seconds for containers to start before running.

echo "=== Initializing Config Server Replica Set ==="
docker exec configsvr mongosh --port 27019 --eval '
rs.initiate({
  _id: "configReplSet",
  configsvr: true,
  members: [{ _id: 0, host: "configsvr:27019" }]
})
'

sleep 3

echo "=== Initializing Shard 1 Replica Set ==="
docker exec shard1 mongosh --port 27018 --eval '
rs.initiate({
  _id: "shard1ReplSet",
  members: [{ _id: 0, host: "shard1:27018" }]
})
'

echo "=== Initializing Shard 2 Replica Set ==="
docker exec shard2 mongosh --port 27018 --eval '
rs.initiate({
  _id: "shard2ReplSet",
  members: [{ _id: 0, host: "shard2:27018" }]
})
'

sleep 5

echo "=== Adding Shards to Cluster ==="
docker exec mongos mongosh --port 27017 --eval '
sh.addShard("shard1ReplSet/shard1:27018");
sh.addShard("shard2ReplSet/shard2:27018");
'

echo "=== Enabling Sharding on university_db ==="
docker exec mongos mongosh --port 27017 --eval '
sh.enableSharding("university_db");
sh.shardCollection("university_db.students", { student_id: "hashed" });
'

echo ""
echo "=== Done! Cluster is ready. ==="
docker exec mongos mongosh --port 27017 --eval 'sh.status()'
