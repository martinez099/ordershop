FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install flask

RUN mkdir -p /app

ENV FLASK_ENV development
ENV FLASK_APP /app/gateway_api/gateway_api.py
ENV PYTHONPATH /app:/app/event_store:/app/message_queue

EXPOSE 5000

RUN git clone https://github.com/martinez099/eventstore.git /app/event_store
RUN git clone https://github.com/martinez099/redismq.git /app/message_queue

CMD [ "flask", "run", "--host=0.0.0.0" ]
