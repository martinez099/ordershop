FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install flask
RUN pip install flask-socketio

RUN mkdir -p /app

COPY api_gateway.py /app/
COPY templates /app/templates
COPY static /app/static

ENV PYTHONPATH /app:/app/event_store:/app/message_queue

EXPOSE 5000

RUN git clone https://github.com/martinez099/eventstore.git /app/event_store
RUN git clone https://github.com/martinez099/redismq.git /app/message_queue

CMD [ "python", "/app/api_gateway.py" ]
