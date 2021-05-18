FROM terminusdb/terminusdb-server:v4.2.1
WORKDIR /app/terminusdb
COPY terminus_init_docker.sh /app/terminusdb/distribution/init_docker.sh
CMD ["/app/terminusdb/distribution/init_docker.sh"]