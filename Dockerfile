FROM openjdk:17-jdk-slim

WORKDIR /app

COPY build/libs/mysql-binlog-kafka-1.0.5.jar app.jar

CMD ["java", "-jar", "app.jar"]