FROM python:3.11
WORKDIR ./user
EXPOSE 8083

COPY . .
COPY requirements.text .

RUN pip install -r requirements.text
CMD [ "airflow", "standalone" ]
