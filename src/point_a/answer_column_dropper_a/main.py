#!/usr/bin/env python3
import os
from common.middleware.filter_rows import FilterRows

FilterRows(
    input_queue='point_a_answers',
    output_queue='answer_bodies',
    output_end_message_count=int(os.getenv('SENTIMENT_CALCULATOR_COUNT')),
    should_discard_row=lambda answer: float(answer['score']) <= 10,
    map_row_to_message_string=lambda answer: answer['body']
).run()
