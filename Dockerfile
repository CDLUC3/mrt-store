#*********************************************************************
#   Copyright 2021 Regents of the University of California
#   All rights reserved
#*********************************************************************

ARG ECR_REGISTRY=ecr_registry_not_set

FROM ${ECR_REGISTRY}/merritt-tomcat:dev

COPY store-war/target/mrt-storewar-*.war /usr/local/tomcat/webapps/store.war

RUN mkdir -p /build/static
RUN date -r /usr/local/tomcat/webapps/store.war +'mrt-store: %Y-%m-%d:%H:%M:%S' > /build/static/build.content.txt 
RUN jar uf /usr/local/tomcat/webapps/store.war -C /build static/build.content.txt

RUN mkdir -p /dpr2store/mrtHomes/store \
    /dpr2store/mrtHomes/logs \
    /opt/storage/ && \
    touch /opt/storage/test.txt
RUN mkdir -p /usr/local/tomcat/webapps/cloudcontainer/store