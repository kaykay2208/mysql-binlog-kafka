
FROM openjdk:11-jre-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN curl -L -u "kaykay2208:<token>" "https://maven.pkg.github.com/kaykay2208/mysql-binlog-kafka/io/kay/mysql/binlogkafka/mysql-binlog-kafka/1.0.3/mysql-binlog-kafka-1.0.3.jar" -o /app/mysql-binlog-kafka-1.0.3.jar
CMD ["java", "-jar", "mysql-binlog-kafka-1.0.3.jar"]

