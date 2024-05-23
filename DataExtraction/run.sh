echo "Build extraction container..."
docker build -t multi_websocket_to_kafka .
docker run --network project-net --name multi_websocket_to_kafka_container multi_websocket_to_kafka
echo "Extraction is started!"
sleep 10
