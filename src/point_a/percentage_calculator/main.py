#!/usr/bin/env python3
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq

with connect_to_rabbitmq() as channel:
    channel.queue_declare(queue='sentiments')
    negative_sentiment_count = 0
    total_sentiment_count = 0
    finished_sentiment_calculator_count = 0
    total_sentiment_calculator_count = int(os.getenv('SENTIMENT_CALCULATOR_COUNT'))


    def handle_sentiment(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        global negative_sentiment_count
        global total_sentiment_count
        global finished_sentiment_calculator_count
        if body == '__end__':
            print('sentiments ended')
            finished_sentiment_calculator_count += 1
            if finished_sentiment_calculator_count == total_sentiment_calculator_count:
                with open('/results/percentage_calculator.txt', 'w+') as file:
                    if total_sentiment_count == 0:
                        file.write('Ended with no answers')
                    else:
                        file.write(f'{100 * negative_sentiment_count / total_sentiment_count} %')
                channel.basic_ack(delivery_tag=method.delivery_tag)
                exit(0)
        else:
            sentiment = float(body)
            total_sentiment_count += 1
            if sentiment < 0:
                negative_sentiment_count += 1
            channel.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_consume(queue='sentiments', on_message_callback=handle_sentiment)
    channel.start_consuming()
