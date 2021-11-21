# USAGE:
#   ruby build_docker_compose.rb <client-count>
# 
# For example, for 3 clients:
#   ruby build_docker_compose.rb 3

File.open('docker-compose.yml', 'w') do |file|
    file.write(
        <<-YAML
version: '3'
services:
  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: rabbitmq.dockerfile
    ports:
      - "15672:15672"
    healthcheck:
        test: ["CMD", "curl", "-f", "http://localhost:15672"]
        interval: 10s
        timeout: 5s
        retries: 10
    logging:
      driver: none

  answers_reader:
    build:
      context: .
      dockerfile: answers_reader/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
    volumes:
      - ../data:/data

  answer_column_dropper_a:
    build:
      context: .
      dockerfile: point_a/answer_column_dropper_a/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - SENTIMENT_CALCULATOR_COUNT

  answer_column_dropper_b:
    build:
      context: .
      dockerfile: point_b/column_dropper_b/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - USER_SCORE_BUCKET_COUNT
      - ROW_TYPE=answer

  question_column_dropper_b:
    build:
      context: .
      dockerfile: point_b/column_dropper_b/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - USER_SCORE_BUCKET_COUNT
      - ROW_TYPE=question

  answer_column_dropper_c:
    build:
      context: .
      dockerfile: point_c/answer_column_dropper_c/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - JOINER_BY_QUESTION_ID_COUNT

  question_column_dropper_c:
    build:
      context: .
      dockerfile: point_c/question_column_dropper_c/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - JOINER_BY_QUESTION_ID_COUNT

  questions_reader:
    build:
      context: .
      dockerfile: questions_reader/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - USER_SCORE_BUCKET_COUNT
      - JOINER_BY_QUESTION_ID_COUNT
    volumes:
      - ../data:/data

  point_a_sentiment_calculator:
    build:
      context: .
      dockerfile: point_a/sentiment_calculator/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
    volumes:
      - ../nltk_data:/root/nltk_data

  point_a_percentage_calculator:
    build:
      context: .
      dockerfile: point_a/percentage_calculator/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - SENTIMENT_CALCULATOR_COUNT
    volumes:
      - ../results:/results

  point_b_average_question_score_calculator:
    build:
      context: .
      dockerfile: point_b/average_score_calculator/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - SCORES_QUEUE_NAME=question_scores
      - AVG_SCORE_EXCHANGE_NAME=avg_question_score
      - USER_SCORE_BUCKET_COUNT

  point_b_average_answer_score_calculator:
    build:
      context: .
      dockerfile: point_b/average_score_calculator/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - SCORES_QUEUE_NAME=answer_scores
      - AVG_SCORE_EXCHANGE_NAME=avg_answer_score
      - USER_SCORE_BUCKET_COUNT

  point_b_top_10_calculator:
    build:
      context: .
      dockerfile: point_b/top_10_calculator/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - USER_SCORE_BUCKET_COUNT
    volumes:
      - ../results:/results

  point_c_file_writer:
    build:
      context: .
      dockerfile: point_c/file_writer/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - JOINER_BY_YEAR_COUNT
    volumes:
      - ../results:/results

        YAML
    )

    (1..ENV["USER_SCORE_BUCKET_COUNT"].to_i).map do |node_id|
        file.write(
            <<-YAML
  point_b_total_question_score_by_user_accumulator#{node_id}:
    build:
      context: .
      dockerfile: point_b/total_score_by_user_accumulator/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_ID=#{node_id - 1}
      - USER_WITH_SCORE_EXCHANGE_NAME=question_user_with_score
      - USER_WITH_TOTAL_SCORE_EXCHANGE_NAME=question_user_with_total_score
      - AVG_SCORE_EXCHANGE_NAME=avg_question_score

  point_b_total_answer_score_by_user_accumulator#{node_id}:
    build:
      context: .
      dockerfile: point_b/total_score_by_user_accumulator/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_ID=#{node_id - 1}
      - USER_WITH_SCORE_EXCHANGE_NAME=answer_user_with_score
      - USER_WITH_TOTAL_SCORE_EXCHANGE_NAME=answer_user_with_total_score
      - AVG_SCORE_EXCHANGE_NAME=avg_answer_score

  point_b_total_score_by_user_joiner#{node_id}:
    build:
      context: .
      dockerfile: point_b/total_score_by_user_joiner/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_ID=#{node_id - 1}

            YAML
        )
    end

    (1..ENV["JOINER_BY_QUESTION_ID_COUNT"].to_i).map do |node_id|
        file.write(
            <<-YAML
  point_c_joiner_by_question_id#{node_id}:
    build:
      context: .
      dockerfile: point_c/joiner_by_question_id/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_ID=#{node_id - 1}
      - JOINER_BY_YEAR_COUNT

            YAML
        )
    end

    (1..ENV["JOINER_BY_YEAR_COUNT"].to_i).map do |node_id|
        file.write(
            <<-YAML
  point_c_joiner_by_year#{node_id}:
    build:
      context: .
      dockerfile: point_c/joiner_by_year/Dockerfile
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - NODE_ID=#{node_id - 1}
      - JOINER_BY_QUESTION_ID_COUNT

            YAML
        )
    end
end
