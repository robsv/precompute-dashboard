#!/bin/sh

docker-compose -f docker-compose-dev.yml down
docker container ls -a | grep chacrmp-app | awk '{print $1}' | xargs docker container rm
docker image ls | grep chacrm2-app | awk '{print $3}' | xargs docker image rm
docker volume rm chacrm2_static_volume
docker-compose -f docker-compose-dev.yml up -d

