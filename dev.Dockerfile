FROM ubuntu:latest
WORKDIR "/opt/stitcher"

RUN apt update
RUN apt-get update
RUN apt-get install -y openjdk-11-jdk
RUN apt-get install sudo
RUN apt-get install nano
RUN sudo apt-get install -y zip
RUN apt-get install apt-transport-https curl gnupg -yqq
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
RUN chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg

RUN apt-get update && apt-get install -y python3-pip
RUN apt-get install -y python3-requests

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

RUN apt-get update
RUN apt-get install -y sbt
EXPOSE 9003


RUN echo "-J-Xms2048M -J-Xmx32G -J-Xss1024M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:+HeapDumpOnOutOfMemoryError -J-XX:HeapDumpPath=./heapdump.hprof" > .sbtopts

RUN alias python='python3'
RUN apt-get install -y python3-pandas

#RUN ./scripts/stitching/stitch-all-current.sh | sudo tee /opt/stitcher/stitch.log
#RUN unzip -o scripts/deployment/*zip

#RUN chmod +x ./scripts/deployment/restart-stitcher-from-repo.sh

#CMD cp -r $(ls -d /opt/app/stitchv*.db) /opt/app/apiDB/; \
#    rm -rf /opt/app/browserDB/*; \
#    cp -r /opt/app/apiDB/$(basename /opt/app/stitchv*.db) /opt/app/browserDB/graph.db; \
#    ./scripts/deployment/restart-stitcher-from-repo.sh $(basename /opt/app/stitchv*.db);
#./scripts/deployment/restart-stitcher-from-repo.sh stitchv1.5
CMD bash

# python3 scripts/stitcher-curation/applyCurations.py dev --filename scripts/stitcher-curation/dbCurations-2023-02-13.txt