FROM postgres:10
ENV POSTGRES_USER docker
ENV POSTGRES_PASSWORD docker
ENV POSTGRES_DB dataproject2
ADD CreateDB.sql /docker-entrypoint-initdb.d/
