FROM haproxy:2.6.6
#linux update + curl installation

USER root
RUN apt-get update -y\
    && ACCEPT_EULA=Y apt-get upgrade -y\
    && apt-get install -y curl\
    && apt-get clean -y
