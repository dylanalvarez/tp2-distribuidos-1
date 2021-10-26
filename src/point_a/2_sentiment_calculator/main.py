#!/usr/bin/env python3
from time import sleep

import pika
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

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
channel.queue_declare(queue='sentiments')

sentiment_analyzer = SentimentIntensityAnalyzer()

def handle_answer_body(_ch, _method, _properties, body):
    body = body.decode("ISO-8859-1")
    if body == '__end__':
        channel.basic_publish(exchange='', routing_key='sentiments', body=body)
        connection.close()
        exit(0)
    else:
        channel.basic_publish(
            exchange='', routing_key='sentiments', body=str(sentiment_analyzer.polarity_scores(str(body))['compound'])
        )


channel.basic_consume(queue='answer_bodies', on_message_callback=handle_answer_body, auto_ack=True)

channel.start_consuming()
