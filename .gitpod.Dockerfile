FROM gitpod/workspace-full

RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.9.3.tgz

RUN tar -zxvf mongodb-database-tools-*.tgz

RUN sudo cp mongodb-database-tools-*/bin/* /usr/local/bin/