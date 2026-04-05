FROM python:3.12-slim

WORKDIR /home/flask-app

COPY orchestrator /tmp/orchestrator
COPY payment/requirements.txt /tmp/payment-requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install --no-build-isolation /tmp/orchestrator \
    && python -m pip install -r /tmp/payment-requirements.txt

COPY payment /home/flask-app

EXPOSE 5000
