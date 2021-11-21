#!/usr/bin/env python3
import os
import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end
from common.get_node_id import get_node_id

with connect_to_rabbitmq() as channel:
    accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))
    channel.queue_declare(queue='point_b_answers')
    channel.queue_declare(queue='answer_scores')
    for node_id in range(accumulator_count):
        channel.queue_declare(queue=f'answer_user_with_score_{node_id}')

    def handle_answer(_ch, method, _properties, body):
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            send_end(channel, exchange='', routing_key='answer_scores')
            for node_id in range(accumulator_count):
                send_end(channel, exchange='', routing_key=f'answer_user_with_score_{node_id}')
            channel.basic_ack(delivery_tag=method.delivery_tag)
            exit(0)
        else:
            answer = json.loads(body)
            channel.basic_publish(exchange='', routing_key='answer_scores', body=answer['score'])
            channel.basic_publish(
                exchange='',
                routing_key=f'answer_user_with_score_{str(get_node_id(answer["user_id"], accumulator_count))}',
                body=' '.join((answer['user_id'], str(answer['score'])))
            )

    channel.basic_consume(queue='point_b_answers', on_message_callback=handle_answer)
    channel.start_consuming()
