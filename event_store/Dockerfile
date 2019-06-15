FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install redis

RUN mkdir -p /app

ENV PYTHONPATH /app

EXPOSE 50051

RUN git clone --recurse-submodules https://github.com/martinez099/eventstore.git /app

CMD [ "python", "/app/event_store_server.py" ]
