FROM postgres:16

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=data_engineer

# Copy init SQL and datasets into the official init folder
COPY datasets/ /docker-entrypoint-initdb.d/

