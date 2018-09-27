ARG SERVICE_NAME

FROM python:3

RUN pip install flask
RUN pip install redis

ENV SERVICE_NAME=order_service

RUN mkdir -p /app/ordershop

ENV FLASK_ENV development
ENV FLASK_APP /app/ordershop/${SERVICE_NAME}/${SERVICE_NAME}.py
ENV PYTHONPATH /app

EXPOSE 5000

VOLUME /app/ordershop

CMD [ "flask", "run", "--host=0.0.0.0" ]
