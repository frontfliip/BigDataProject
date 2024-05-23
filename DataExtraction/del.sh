echo "Stop extraction container..."
docker stop multi_websocket_to_kafka_container
echo "Delete extraction container..."
docker rm multi_websocket_to_kafka_container
sleep 2
