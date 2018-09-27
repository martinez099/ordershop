FROM python:3

RUN pip install redis
RUN pip install requests

RUN mkdir -p /app/ordershop

ENV PYTHONPATH /app

EXPOSE 5000

VOLUME /app/ordershop

CMD [ "python", "/app/ordershop/crm_service/crm_service.py" ]
