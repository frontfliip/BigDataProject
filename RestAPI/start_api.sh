docker build -f RestAPI/Dockerfile.api . -t restapi
sleep 5
#docker run --name rest_api --network project-net -p 1488:1488 restapi
#docker run --name rest_api --network project-net restapi
#sleep 20
#use this one to see logs
docker run --name rest_api --network project-net -p 1488:1488 restapi
