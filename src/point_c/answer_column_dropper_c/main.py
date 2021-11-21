#!/usr/bin/env python3
import os

from common.middleware.filter_rows import FilterRows
from common.get_node_id import get_node_id

joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))

end_message_count_by_output_queue = {}
for node_id in range(joiner_by_question_id_count):
    end_message_count_by_output_queue[f'answer_id_with_year_score_{node_id}'] = 1

FilterRows(
    input_queue='point_c_answers',
    map_row_to_message_by_output_queue=lambda answer: {
        f'answer_id_with_year_score_{str(get_node_id(answer["question_id"], joiner_by_question_id_count))}': ' '.join((answer["question_id"], answer["creation_date"][0:4], answer["score"]))
    },
    end_message_count_by_output_queue=end_message_count_by_output_queue
).run()
