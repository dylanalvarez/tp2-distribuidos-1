#!/usr/bin/env python3
import os
from common.middleware.filter_rows import FilterRows
from common.get_node_id import get_node_id
from common.connect_to_rabbitmq import connect_to_rabbitmq
from common.middleware.queue import Queue

row_type = os.getenv('ROW_TYPE')
accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))

with connect_to_rabbitmq() as channel:
    input_queue = Queue(f'point_b_{row_type}s', channel)
    scores_queue = Queue(f'{row_type}_scores', channel)

    user_with_score_queues_by_node_id = {}
    end_message_count_by_output_queue = {scores_queue: 1}

    for node_id in range(accumulator_count):
        queue = Queue(f'{row_type}_user_with_score_{node_id}', channel)
        user_with_score_queues_by_node_id[str(node_id)] = queue
        end_message_count_by_output_queue[queue] = 1

    FilterRows(
        input_queue=input_queue,
        map_row_to_message_by_output_queue=lambda row: {
            scores_queue:
                row['score'],
            user_with_score_queues_by_node_id[get_node_id(row["user_id"], accumulator_count)]:
                ' '.join((row['user_id'], str(row['score'])))
        },
        end_message_count_by_output_queue=end_message_count_by_output_queue
    ).run()
