FROM python:3.8

ENV PATH=$PATH:/var/lib/postgresql/.local/bin

RUN apt-get update \
  && apt-get install -y postgresql postgresql-contrib \
  && apt-get install sudo \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /home/postgresql/data/ && chown -R postgres:postgres /home/postgresql/data/ && chmod 750 /home/postgresql/data/
USER postgres
RUN pip install psycopg2-binary patroni[etcd]
