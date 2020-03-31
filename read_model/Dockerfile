FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install redis

RUN mkdir -p /app

COPY read_model.py /app/

ENV PYTHONPATH /app:/app/domain_model:/app/event_store:/app/message_queue

RUN git clone https://github.com/martinez099/domainmodel.git /app/domain_model
RUN git clone https://github.com/martinez099/eventstore.git /app/event_store
RUN git clone https://github.com/martinez099/redismq.git /app/message_queue

CMD [ "python", "app/read_model.py" ]
