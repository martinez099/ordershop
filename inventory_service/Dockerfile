ARG SERVICE_NAME

FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools
RUN pip install flask

ENV SERVICE_NAME=inventory_service

RUN mkdir -p /app

ENV FLASK_ENV development
ENV FLASK_APP /app/${SERVICE_NAME}/${SERVICE_NAME}.py
ENV PYTHONPATH /app

EXPOSE 5000

CMD [ "flask", "run", "--host=0.0.0.0" ]
