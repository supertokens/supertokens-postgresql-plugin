FROM ubuntu:16.04

RUN apt-get update && apt-get upgrade -y

RUN apt-get install build-essential -y
RUN  apt-get install -y wget

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8

RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" > /etc/apt/sources.list.d/pgdg.list


RUN apt-get update && apt-get -y -q install python-software-properties software-properties-common \
    && apt-get -y -q install postgresql-9.5 postgresql-client-9.5 postgresql-contrib-9.5

USER postgres

RUN /etc/init.d/postgresql start \
    && psql --command "CREATE USER root WITH SUPERUSER PASSWORD 'root';" \
    && createdb -O root root \
    && createdb -O root auth_session

USER root

RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/9.5/main/pg_hba.conf


RUN echo "listen_addresses='*'" >> /etc/postgresql/9.5/main/postgresql.conf


RUN mkdir -p /var/run/postgresql && chown -R postgres /var/run/postgresql

VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql"]



# Install OpenJDK 12
RUN wget https://download.java.net/java/GA/jdk12.0.2/e482c34c86bd4bf8b56c0b35558996b9/10/GPL/openjdk-12.0.2_linux-x64_bin.tar.gz

RUN mkdir /usr/java

RUN mv openjdk-12.0.2_linux-x64_bin.tar.gz /usr/java

RUN cd /usr/java && tar -xzvf openjdk-12.0.2_linux-x64_bin.tar.gz

RUN echo 'JAVA_HOME=/usr/java/jdk-12.0.2' >> /etc/profile
RUN echo 'PATH=$PATH:$HOME/bin:$JAVA_HOME/bin' >> /etc/profile
RUN echo 'export JAVA_HOME' >> /etc/profile
RUN echo 'export JRE_HOME' >> /etc/profile
RUN echo 'export PATH' >> /etc/profile

RUN update-alternatives --install "/usr/bin/java" "java" "/usr/java/jdk-12.0.2/bin/java" 1
RUN update-alternatives --install "/usr/bin/javac" "javac" "/usr/java/jdk-12.0.2/bin/javac" 1

RUN apt-get install jq -y

RUN apt-get install curl -y

ADD ./runPostgreSQL.sh /runPostgreSQL.sh

RUN chmod +x /runPostgreSQL.sh

RUN apt-get install -y git-core