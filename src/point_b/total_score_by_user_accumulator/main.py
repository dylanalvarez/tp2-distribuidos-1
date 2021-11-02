#!/usr/bin/env python3
import math
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    node_id = os.getenv('NODE_ID')
    user_with_score_exchange_name = os.getenv('USER_WITH_SCORE_EXCHANGE_NAME')
    user_with_total_score_exchange_name = os.getenv('USER_WITH_TOTAL_SCORE_EXCHANGE_NAME')
    avg_score_exchange_name = os.getenv('AVG_SCORE_EXCHANGE_NAME')

    user_with_score_queue_name = f'{user_with_score_exchange_name}_{node_id}'
    channel.queue_declare(queue=user_with_score_queue_name)

    channel.queue_declare(queue=f'{user_with_total_score_exchange_name}_{node_id}')

    avg_score_queue_name = f'{avg_score_exchange_name}_{node_id}'
    channel.queue_declare(queue=avg_score_queue_name)

    scores_by_user_id = {}
    has_all_users = False
    average_score = None

    def enqueue_if_finished():
        if average_score is None or not has_all_users:
            return

        for user_id, (score_count, score_sum) in scores_by_user_id.items():
            if score_sum / score_count >= average_score:
                channel.basic_publish(exchange='', routing_key=f'{user_with_total_score_exchange_name}_{node_id}', body=' '.join((user_id, str(score_sum))))
        send_end(channel, exchange='', routing_key=f'{user_with_total_score_exchange_name}_{node_id}')
        exit(0)


    def handle_user_with_score(_ch, method, _properties, body):
        global scores_by_user_id
        global has_all_users
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            has_all_users = True
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            user_id, score = body.split(' ')
            score = float(score)
            if not math.isnan(score):
                if user_id in scores_by_user_id:
                    score_count, score_sum = scores_by_user_id[user_id]
                    scores_by_user_id[user_id] = (score_count + 1, score_sum + score)
                else:
                    scores_by_user_id[user_id] = (1, score)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        enqueue_if_finished()


    channel.basic_consume(queue=user_with_score_queue_name, on_message_callback=handle_user_with_score)

    def handle_avg_score(_ch, method, _properties, body):
        global average_score
        average_score = float(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        enqueue_if_finished()


    channel.basic_consume(queue=avg_score_queue_name, on_message_callback=handle_avg_score)

    channel.start_consuming()
