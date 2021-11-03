#!/bin/bash
export $(cat .env | sed 's/#.*//g' | xargs)
docker build -f base-images/python-base.dockerfile -t rabbitmq-python-base:0.0.1 .
touch docker-compose.yml
docker-compose -f docker-compose-scripts.yml up --build build_docker_compose
docker-compose up --build --scale point_a_sentiment_calculator=$SENTIMENT_CALCULATOR_COUNT
docker-compose stop