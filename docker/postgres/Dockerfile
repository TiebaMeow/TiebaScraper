FROM postgres:17

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-17-partman \
    && rm -rf /var/lib/apt/lists/*

COPY init-pg-partman.sql /docker-entrypoint-initdb.d/10-init-pg-partman.sql
