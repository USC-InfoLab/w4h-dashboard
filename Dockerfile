FROM python:3.8-slim

RUN apt update && apt upgrade
RUN pip install --upgrade pip

RUN apt install postgresql postgresql-contrib -y
RUN apt install postgis -y

VOLUME /var/lib/postgresql/data

WORKDIR /app

COPY . .

RUN apt-get install default-jdk -y
RUN pip install jupyter
RUN pip install pyspark
RUN pip install -r requirements.txt
RUN apt-get install vim -y
RUN apt-get install sudo
RUN chmod +x /app/inituser_and_start.sh

EXPOSE 8051
EXPOSE 5432
EXPOSE 8888

ENV POSTGRES_DB admin
ENV POSTGRES_USER admin
ENV POSTGRES_PASSWORD admin
ENV JAVA_HOME /usr/lib/jvm/java-1.17.0-openjdk-amd64

CMD ["./inituser_and_start.sh"]