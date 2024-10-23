FROM ubuntu:latest as buildContainer
WORKDIR "/opt/stitcher"

RUN apt update
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install sudo
RUN apt-get install nano
RUN sudo apt-get install graphviz -y
RUN sudo apt-get install -y zip
RUN apt-get install apt-transport-https curl gnupg -yqq
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
RUN chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg

RUN sudo apt-get install pip -y
RUN sudo pip install requests
RUN sudo pip install openpyxl

RUN sh -c "$(curl -fsSL https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/install-edirect.sh) | sh -s y"

RUN apt-get update
RUN apt-get install sbt
EXPOSE 9003

RUN echo "-J-Xms2048M -J-Xmx32G -J-Xss1024M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:+HeapDumpOnOutOfMemoryError -J-XX:HeapDumpPath=./heapdump.hprof" > .sbtopts

RUN alias python='python3'

RUN pip install pandas
RUN pip install snakemake
RUN pip install pulp==2.7
RUN pip install xlrd

ENV SBT_OPTS="-Dsbt.rootdir=true"
CMD export PATH=${HOME}/edirect:${PATH} && bash
#CMD snakemake --cores=all
