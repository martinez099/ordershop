FROM python:3

RUN mkdir -p /app

ENV PYTHONPATH /app

EXPOSE 50051

RUN git clone --recurse-submodules https://github.com/martinez099/redismq.git /app
RUN pip install -r app/requirements.txt

CMD [ "python", "/app/message_queue_server.py" ]
