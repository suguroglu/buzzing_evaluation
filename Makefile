help:
        @cat Makefile

DOCKER_FILE=Dockerfile
TEST=tests/
SRC=$(shell dirname `pwd`)

build:
        sudo docker build -t buzzing_evaluation  -f $(DOCKER_FILE) .

build_force:
        sudo docker build --no-cache -t buzzing_evaluation -f $(DOCKER_FILE) .
run:
        sudo docker run --rm --env-file ./env.file --name buzzing_container -t buzzing_evaluation python ts_analysis.py