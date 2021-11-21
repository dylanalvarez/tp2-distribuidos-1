#!/usr/bin/env python3
import os
from common.middleware.filter_rows import FilterRows
from common.middleware.queue import Queue
from common.connect_to_rabbitmq import connect_to_rabbitmq

with connect_to_rabbitmq() as channel:
    input_queue = Queue('point_a_answers', channel)
    answer_bodies_queue = Queue('answer_bodies', channel)
    FilterRows(
        input_queue=input_queue,
        map_row_to_message_by_output_queue=lambda answer: {
            answer_bodies_queue: answer['body']
        },
        end_message_count_by_output_queue={
            answer_bodies_queue: int(os.getenv('SENTIMENT_CALCULATOR_COUNT'))
        },
        should_discard_row=lambda answer: float(answer['score']) <= 10,
    ).run()
