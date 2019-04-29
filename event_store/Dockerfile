FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install redis

RUN mkdir -p /app

ENV PYTHONPATH /app

EXPOSE 50051

CMD [ "python", "/app/event_store/event_store.py" ]
