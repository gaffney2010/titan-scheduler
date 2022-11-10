FROM ubuntu:latest
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    python3-scipy

# git is needed for requirements.txt
RUN apt-get install -y git

# Needed to read from hosted github
RUN apt-get install -y mysql-server
RUN apt-get install -y libmysqlclient-dev

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./*.py .
COPY secrets.yaml .
RUN mkdir dags
COPY dags/*.py dags
RUN mkdir shared
COPY shared/*.py shared
RUN mkdir titan_server
COPY titan_server/*.py titan_server

CMD ["python3", "titan_server.py"]
