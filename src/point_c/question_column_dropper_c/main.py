#!/usr/bin/env python3
import os

from common.middleware.filter_rows import FilterRows
from common.get_node_id import get_node_id

joiner_by_question_id_count = int(os.getenv('JOINER_BY_QUESTION_ID_COUNT'))

end_message_count_by_output_queue = {}
for node_id in range(joiner_by_question_id_count):
    end_message_count_by_output_queue[f'question_id_with_year_score_{node_id}'] = 1

FilterRows(
    input_queue='point_c_questions',
    map_row_to_message_by_output_queue=lambda question: {
        f'question_id_with_year_score_{str(get_node_id(question["question_id"], joiner_by_question_id_count))}': ' '.join((question["question_id"], question["creation_date"][0:4], question["score"], question["tags"]))
    },
    end_message_count_by_output_queue=end_message_count_by_output_queue
).run()
