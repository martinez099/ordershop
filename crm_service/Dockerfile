FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install requests

RUN mkdir -p /app

ENV PYTHONPATH /app:/app/event_store

RUN git clone https://github.com/martinez099/eventstore.git /app/event_store

CMD [ "python", "/app/crm_service/crm_service.py" ]
