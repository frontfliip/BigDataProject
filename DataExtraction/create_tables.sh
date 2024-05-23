echo "Creating Tables ..."

while [ "$(docker inspect --format='{{.State.Health.Status}}' cassandra)" != "healthy" ]; do
  echo "Waiting for Cassandra to be healthy..."
  sleep 10
done

docker exec -i cassandra cqlsh -e "$(cat DataExtraction/ddl.cql)"

echo "Tables created!"
sleep 10
