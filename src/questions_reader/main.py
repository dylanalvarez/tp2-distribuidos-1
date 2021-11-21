#!/usr/bin/env python3
import csv
import json

from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.send_end import send_end

with connect_to_rabbitmq() as channel:
    channel.queue_declare(queue='point_b_questions')
    channel.queue_declare(queue='point_c_questions')

    with open('/data/questions.csv', encoding="ISO-8859-1") as file:
        questions = csv.reader(file)
        _header = next(questions)
        for question in questions:
            _, id, user_id, creation_date, closed_date, score, title, body, tags = question
            question_json = json.dumps({
                'question_id': id,
                'user_id': user_id,
                'creation_date': creation_date,
                'closed_date': closed_date,
                'score': score,
                'title': title,
                'body': body,
                'tags': tags,
            })
            channel.basic_publish(exchange='', routing_key='point_b_questions', body=question_json)
            channel.basic_publish(exchange='', routing_key='point_c_questions', body=question_json)

    send_end(channel, exchange='', routing_key='point_b_questions')
    send_end(channel, exchange='', routing_key='point_c_questions')
