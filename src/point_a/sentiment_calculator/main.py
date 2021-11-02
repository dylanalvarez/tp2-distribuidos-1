#!/usr/bin/env python3

import nltk
from common.connect_to_rabbitmq import connect_to_rabbitmq
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# nltk.download('vader_lexicon')

with connect_to_rabbitmq() as channel:
    channel.queue_declare(queue='answer_bodies')
    channel.queue_declare(queue='sentiments')
    sentiment_analyzer = SentimentIntensityAnalyzer()


    def handle_answer_body(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            channel.basic_publish(exchange='', routing_key='sentiments', body=body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            exit(0)
        else:
            channel.basic_publish(
                exchange='', routing_key='sentiments',
                body=str(sentiment_analyzer.polarity_scores(str(body))['compound'])
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_consume(queue='answer_bodies', on_message_callback=handle_answer_body)
    channel.start_consuming()
