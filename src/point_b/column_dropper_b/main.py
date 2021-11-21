#!/usr/bin/env python3
import os
from common.middleware.filter_rows import FilterRows
from common.get_node_id import get_node_id

row_type = os.getenv('ROW_TYPE')
accumulator_count = int(os.getenv('USER_SCORE_BUCKET_COUNT'))

end_message_count_by_output_queue = {f'{row_type}_scores': 1}
for node_id in range(accumulator_count):
    end_message_count_by_output_queue[f'{row_type}_user_with_score_{node_id}'] = 1

FilterRows(
    input_queue=f'point_b_{row_type}s',
    map_row_to_message_by_output_queue=lambda row: {
        f'{row_type}_scores': row['score'],
        f'{row_type}_user_with_score_{str(get_node_id(row["user_id"], accumulator_count))}': ' '.join((row['user_id'], str(row['score'])))
    },
    end_message_count_by_output_queue=end_message_count_by_output_queue
).run()
