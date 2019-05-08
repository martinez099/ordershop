ARG SERVICE_NAME

FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install flask

ENV SERVICE_NAME=customer_service

RUN mkdir -p /app

ENV FLASK_ENV development
ENV FLASK_APP /app/${SERVICE_NAME}/${SERVICE_NAME}.py
ENV PYTHONPATH /app:/app/event_store

EXPOSE 5000

RUN git clone https://github.com/martinez099/eventstore.git /app/event_store

CMD [ "flask", "run", "--host=0.0.0.0" ]
