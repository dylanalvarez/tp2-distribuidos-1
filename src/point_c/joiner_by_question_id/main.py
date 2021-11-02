#!/usr/bin/env python3
import math
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end
from common.get_node_id import get_node_id

with connect_to_rabbitmq() as channel:
    node_id = os.getenv('NODE_ID')
    joiner_by_year_count = int(os.getenv('JOINER_BY_YEAR_COUNT'))

    channel.queue_declare(queue=f'answer_id_with_year_score_{node_id}')
    channel.queue_declare(queue=f'question_id_with_year_score_{node_id}')
    for _node_id in range(joiner_by_year_count):
        channel.queue_declare(queue=f'year_score_sum_tags_{_node_id}')

    question_queue_ended = False
    answer_queue_ended = False

    tags_by_question_id = {}
    score_sum_by_year_by_question_id = {}

    def add_score(question_id, year, score):
        score = float(score)
        score = 0 if math.isnan(score) else score
        if question_id in score_sum_by_year_by_question_id:
            score_sum_by_year_by_question_id[question_id][year] = score_sum_by_year_by_question_id[question_id].get(year, 0) + score
        else:
            score_sum_by_year_by_question_id[question_id] = {year: score}

    def enqueue_if_finished():
        if not question_queue_ended or not answer_queue_ended:
            return

        for question_id, score_by_year in score_sum_by_year_by_question_id.items():
            for year, score in score_by_year.items():
                if question_id in tags_by_question_id:
                    body = ' '.join((year, str(score), *tags_by_question_id[question_id]))
                    channel.basic_publish(exchange='', routing_key=f'year_score_sum_tags_{get_node_id(year, joiner_by_year_count)}', body=body)
        for joiner_by_year_node_id in range(joiner_by_year_count):
            send_end(channel, exchange='', routing_key=f'year_score_sum_tags_{joiner_by_year_node_id}')
        exit(0)

    def handle_answer_id_with_year_score(_ch, method, _properties, body):
        global answer_queue_ended
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            answer_queue_ended = True
            channel.basic_ack(delivery_tag=method.delivery_tag)
            enqueue_if_finished()
        else:
            question_id, year, score = body.split(' ')
            add_score(question_id, year, score)
            channel.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=f'answer_id_with_year_score_{node_id}', on_message_callback=handle_answer_id_with_year_score)

    def handle_question_id_with_year_score(_ch, method, _properties, body):
        global question_queue_ended
        body = body.decode("ISO-8859-1")
        if body == '__end__':
            question_queue_ended = True
            channel.basic_ack(delivery_tag=method.delivery_tag)
            enqueue_if_finished()
        else:
            question_id, year, score, *tags = body.split(' ')
            tags_by_question_id[question_id] = tags
            add_score(question_id, year, score)
            channel.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=f'question_id_with_year_score_{node_id}', on_message_callback=handle_question_id_with_year_score)

    channel.start_consuming()
