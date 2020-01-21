FROM python:3

RUN mkdir -p /app

ENV PYTHONPATH /app

RUN git clone https://github.com/martinez099/eventstore.git /app
RUN pip install -r app/requirements.txt

EXPOSE 50051

CMD [ "python", "/app/event_store_server.py" ]
