# Cassandra PoC

Some code I use to play with Cassandra.

## Start cassandra using docker

Start the first node
> docker run -v /home/ggye/docker-data/cassandra:/var/lib/cassandra -p 9042:9042 --name c1 -d cassandra:3.11.8

Start the second node and connect it to the first one
> docker run -v /home/ggye/docker-data/cassandra2:/var/lib/cassandra --name c2 -d -e CASSANDRA_SEEDS="$(docker inspect --format='{{.NetworkSettings.IPAddress }}' c1)" cassandra:3.11.8

Check the status of the cluster
> docker exec -i -t c1 bash -c 'nodetool status'


