#!/usr/bin/env python3
import os
import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    sentiment_calculator_count = int(os.getenv('SENTIMENT_CALCULATOR_COUNT'))
    channel.queue_declare(queue='point_a_answers')
    channel.queue_declare(queue='answer_bodies')

    def handle_answer(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            for _ in range(sentiment_calculator_count):
                send_end(channel, exchange='', routing_key='answer_bodies')
            channel.basic_ack(delivery_tag=method.delivery_tag)
            exit(0)
        else:
            answer = json.loads(body)
            if float(answer['score']) > 10:
                channel.basic_publish(exchange='', routing_key='answer_bodies', body=answer['body'])

    channel.basic_consume(queue='point_a_answers', on_message_callback=handle_answer)
    channel.start_consuming()
