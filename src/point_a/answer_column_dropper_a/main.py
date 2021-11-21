#!/usr/bin/env python3
import os
from common.middleware.filter_rows import FilterRows

FilterRows(
    input_queue='point_a_answers',
    map_row_to_message_by_output_queue=lambda answer: {
        'answer_bodies': answer['body']
    },
    end_message_count_by_output_queue={
        'answer_bodies': int(os.getenv('SENTIMENT_CALCULATOR_COUNT'))
    },
    should_discard_row=lambda answer: float(answer['score']) <= 10,
).run()
