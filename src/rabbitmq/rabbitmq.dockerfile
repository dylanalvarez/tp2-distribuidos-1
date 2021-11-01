FROM rabbitmq:3.9.8-management
RUN apt-get update
RUN apt-get install -y curl