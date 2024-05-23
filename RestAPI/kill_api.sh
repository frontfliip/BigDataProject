echo "Stopping REST API..."
docker stop rest_api
echo "Removing REST API container..."
docker rm rest_api
echo "Deleting is Done!"
sleep 5
