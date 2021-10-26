#!/usr/bin/env python3
import os
from time import sleep

import csv
import pika

connection = None
for _ in range(20):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    except pika.exceptions.AMQPConnectionError:
        sleep(1)
if not connection:
    exit(1)

channel = connection.channel()

channel.queue_declare(queue='answer_bodies')

sentiment_calculator_count = int(os.getenv('SENTIMENT_CALCULATOR_COUNT'))

with open('/data/answers.csv', encoding="ISO-8859-1") as file:
    answers = csv.reader(file)
    _header = next(answers)
    for answer in answers:
        _, answer_id, user_id, creation_date, question_id, score, body = answer
        if float(score) > 10:
            channel.basic_publish(exchange='', routing_key='answer_bodies', body=body)

for _ in range(sentiment_calculator_count):
    channel.basic_publish(exchange='', routing_key='answer_bodies', body='__end__')

connection.close()
