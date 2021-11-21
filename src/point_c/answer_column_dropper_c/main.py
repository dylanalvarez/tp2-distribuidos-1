#!/usr/bin/env python3
import os

from common.middleware.filter_rows import FilterRows
from common.get_node_id import get_node_id
from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.middleware.queue import Queue

joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))

with connect_to_rabbitmq() as channel:
    answer_id_with_year_queues_by_node_id = {}
    end_message_count_by_output_queue = {}

    input_queue = Queue('point_c_answers', channel)

    for node_id in range(joiner_by_question_id_count):
        queue = Queue(f'answer_id_with_year_score_{node_id}', channel)
        answer_id_with_year_queues_by_node_id[str(node_id)] = queue
        end_message_count_by_output_queue[queue] = 1

    FilterRows(
        input_queue=input_queue,
        map_row_to_message_by_output_queue=lambda answer: {
            answer_id_with_year_queues_by_node_id[get_node_id(answer["question_id"], joiner_by_question_id_count)]:
                ' '.join((answer["question_id"], answer["creation_date"][0:4], answer["score"]))
        },
        end_message_count_by_output_queue=end_message_count_by_output_queue
    ).run()
