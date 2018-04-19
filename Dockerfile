FROM docker-dev.artifactory.dunnhumby.com/centos

ENV http_proxy=http://ukproxy1.dunnhumby.co.uk:8080
ENV https_proxy=https://ukproxy1.dunnhumby.co.uk:8080

ENV no_proxy=".dunnhumby.com,.dunnhumby.co.uk,local,localhost"

RUN yum install -y wget java-1.8.0-openjdk zip unzip
RUN curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo
RUN yum install -y sbt
