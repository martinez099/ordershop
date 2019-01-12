FROM python:3

RUN pip install redis
RUN pip install requests

RUN mkdir -p /app

ENV PYTHONPATH /app

EXPOSE 5000

CMD [ "python", "/app/crm_service/crm_service.py" ]
