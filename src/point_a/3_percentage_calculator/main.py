#!/usr/bin/env python3
import os
from time import sleep

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

channel.queue_declare(queue='sentiments')

negative_sentiment_count = 0
total_sentiment_count = 0
finished_sentiment_calculator_count = 0
total_sentiment_calculator_count = int(os.getenv('SENTIMENT_CALCULATOR_COUNT'))


def handle_sentiment(_ch, _method, _properties, body):
    body = body.decode("ISO-8859-1")
    global negative_sentiment_count
    global total_sentiment_count
    global finished_sentiment_calculator_count
    if body == '__end__':
        finished_sentiment_calculator_count += 1
        if finished_sentiment_calculator_count == total_sentiment_calculator_count:
            if total_sentiment_count == 0:
                print('ended with no answers')
            else:
                print(f'{100 * negative_sentiment_count / total_sentiment_count}%')
            connection.close()
            exit(0)
    else:
        sentiment = float(body)
        total_sentiment_count += 1
        if sentiment < 0:
            negative_sentiment_count += 1


channel.basic_consume(queue='sentiments', on_message_callback=handle_sentiment, auto_ack=True)

channel.start_consuming()
