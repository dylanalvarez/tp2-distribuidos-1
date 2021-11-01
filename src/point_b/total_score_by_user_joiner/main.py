#!/usr/bin/env python3
import math
import os

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    node_id = os.getenv('NODE_ID')
    question_user_with_total_score_queue_name = f'question_user_with_total_score_{node_id}'
    channel.queue_declare(queue=question_user_with_total_score_queue_name)

    answer_user_with_total_score_queue_name = f'answer_user_with_total_score_{node_id}'
    channel.queue_declare(queue=answer_user_with_total_score_queue_name)

    channel.queue_declare(queue='user_with_joined_score')

    scores_by_user_id = {}
    has_all_question_users = False
    has_all_answer_users = False

    def enqueue_remaining_users():
        for user_id, scores in scores_by_user_id.items():
            print(f"sent with end: {' '.join((user_id, str(scores.get('answer', 0) + scores.get('question', 0))))}")
            channel.basic_publish(
                exchange='',
                routing_key='user_with_joined_score',
                body=' '.join((user_id, str(scores.get('answer', 0) + scores.get('question', 0))))
            )
        send_end(channel, exchange='', routing_key='user_with_joined_score')
        exit(0)


    def handle_user(method, body, is_answer):
        global has_all_answer_users
        global has_all_question_users
        global answer_user_with_total_score_queue_name
        global question_user_with_total_score_queue_name
        body = body.decode("ISO-8859-1")
        print(f'received {body}')
        if body == '__end__':
            print(f'received end, is answer: {is_answer}')
            # TODO: it's receiving __end__ of answer users before receiving all send users (is queue order not guaranteed...?)
            if is_answer:
                has_all_answer_users = True
            else:
                has_all_question_users = True
            print(f'has all answer: {has_all_answer_users} has all question: {has_all_question_users}')
            if has_all_answer_users and has_all_question_users:
                enqueue_remaining_users()
        else:
            user_id, score = body.split(' ')
            if user_id not in scores_by_user_id:
                scores_by_user_id[user_id] = {}
            score = float(score)
            score = 0 if math.isnan(score) else score
            key = 'answer' if is_answer else 'question'
            scores_by_user_id[user_id][key] = score
            if 'answer' in scores_by_user_id[user_id] and 'question' in scores_by_user_id[user_id]:
                scores = scores_by_user_id.pop(user_id)
                print(f"sent before end: {' '.join((user_id, str(scores['answer'] + scores['question'])))}")
                channel.basic_publish(exchange='', routing_key='user_with_joined_score', body=' '.join((user_id, str(scores['answer'] + scores['question']))))
        channel.basic_ack(delivery_tag=method.delivery_tag)


    def handle_question_user(_ch, method, _properties, body):
        handle_user(method, body, is_answer=False)


    channel.basic_consume(queue=question_user_with_total_score_queue_name, on_message_callback=handle_question_user)

    def handle_answer_user(_ch, method, _properties, body):
        handle_user(method, body, is_answer=True)


    channel.basic_consume(queue=answer_user_with_total_score_queue_name, on_message_callback=handle_answer_user)

    channel.start_consuming()
