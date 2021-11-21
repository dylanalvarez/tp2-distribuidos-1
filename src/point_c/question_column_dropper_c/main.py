#!/usr/bin/env python3
import os

from common.middleware.filter_rows import FilterRows
from common.get_node_id import get_node_id
from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.middleware.queue import Queue

joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))

with connect_to_rabbitmq() as channel:
    question_id_with_year_queues_by_node_id = {}
    end_message_count_by_output_queue = {}

    input_queue = Queue('point_c_questions', channel)

    for node_id in range(joiner_by_question_id_count):
        queue = Queue(f'question_id_with_year_score_{node_id}', channel)
        question_id_with_year_queues_by_node_id[str(node_id)] = queue
        end_message_count_by_output_queue[queue] = 1

    FilterRows(
        input_queue=input_queue,
        map_row_to_message_by_output_queue=lambda question: {
            question_id_with_year_queues_by_node_id[get_node_id(question["question_id"], joiner_by_question_id_count)]:
                ' '.join((question["question_id"], question["creation_date"][0:4], question["score"], question["tags"]))
        },
        end_message_count_by_output_queue=end_message_count_by_output_queue
    ).run()
