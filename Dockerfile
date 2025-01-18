FROM openjdk:17-jdk-slim

WORKDIR /app

COPY build/libs/mysql-binlog-kafka-1.0.6.jar app.jar

CMD ["java", "-jar", "app.jar"]