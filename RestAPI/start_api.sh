echo "Building Rest API Dockerfile"
docker build -f RestAPI/Dockerfile.api . -t restapi
echo "cool"
echo "Creating network for Rest API"
sleep 5

docker run --name rest_api --network project-net -p 1488:1488 restapi
