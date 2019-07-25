FROM python:3

RUN mkdir -p /app

ENV PYTHONPATH /app

RUN git clone --recurse-submodules https://github.com/martinez099/redismq.git /app
RUN pip install -r app/requirements.txt

EXPOSE 50051

CMD [ "python", "/app/message_queue_server.py" ]
