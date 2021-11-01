#!/usr/bin/env python3
import math
import os
import time

from common.connect_to_rabbitmq import connect_to_rabbitmq

with connect_to_rabbitmq() as channel:
    scores_queue_name = os.getenv('SCORES_QUEUE_NAME')
    avg_score_exchange_name = os.getenv('AVG_SCORE_EXCHANGE_NAME')
    accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))
    channel.queue_declare(queue=scores_queue_name)
    channel.queue_declare(queue=avg_score_exchange_name)

    score_sum = 0
    total_count = 0

    def handle_sentiment(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        global score_sum
        global total_count
        if body == '__end__':
            print(f'{scores_queue_name} ended')
            average_score = 0 if total_count == 0 else score_sum / total_count
            for node_id in range(accumulator_count):
                channel.basic_publish(exchange='', routing_key=f'{avg_score_exchange_name}_{node_id}', body=str(average_score))
            channel.basic_ack(delivery_tag=method.delivery_tag)
            exit(0)
        else:
            score = float(body)
            score_sum += 0 if math.isnan(score) else score
            total_count += 1
            channel.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=scores_queue_name, on_message_callback=handle_sentiment)
    channel.start_consuming()
