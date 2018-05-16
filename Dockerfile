FROM fedora:27

RUN yum -y update && yum -y install java-1.8.0-openjdk-devel && yum -y clean all

ENV JAVA_HOME /usr/lib/jvm/java

ADD target/stream-app.jar /

CMD ["java", "-Dvertx.cacheDirBase=/tmp/src/target", "-jar", "openshiftShowCase-bankApp-1.0-SNAPSHOT-jar-with-dependencies.jar"]