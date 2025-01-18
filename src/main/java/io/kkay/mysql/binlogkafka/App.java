package io.kkay.mysql.binlogkafka;


import io.kkay.mysql.binlogkafka.config.KafkaConfig;
import io.kkay.mysql.binlogkafka.config.MySQLBinlogConfig;
import io.kkay.mysql.binlogkafka.listener.BinlogEventListener;
import io.kkay.mysql.binlogkafka.producer.KafkaProducerService;
import io.kkay.mysql.binlogkafka.service.MySQLBinlogService;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

public class App {

    public static void main(String[] args) {
        String mysqlHost = System.getenv("MYSQL_HOST"); // MySQL host
        String mysqlPort = System.getenv("MYSQL_PORT"); // MySQL port
        String mysqlUser = System.getenv("MYSQL_USER"); // MySQL user
        String mysqlPassword = System.getenv("MYSQL_PASSWORD"); // MySQL password

        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS"); // Kafka bootstrap servers
        String kafkaTopic = System.getenv("KAFKA_TOPIC"); // Kafka topic

        // Default values in case environment variables are not set
        if (mysqlHost == null) mysqlHost = "localhost";
        if (mysqlPort == null) mysqlPort = "3306";
        if (mysqlUser == null) mysqlUser = "root";
        if (mysqlPassword == null) mysqlPassword = "";
        if (kafkaBootstrapServers == null) kafkaBootstrapServers = "localhost:9092";
        if (kafkaTopic == null) kafkaTopic = "mysql-binlog-topic";


        KafkaProducer<String, String> kafkaProducer = KafkaConfig.createKafkaProducer(kafkaBootstrapServers);
        KafkaProducerService kafkaProducerService = new KafkaProducerService(kafkaProducer, kafkaTopic);


        BinaryLogClient binlogClient = MySQLBinlogConfig.createBinlogClient(mysqlHost, Integer.valueOf(mysqlPort), mysqlUser, mysqlPassword);
        BinlogEventListener listener = new BinlogEventListener(kafkaProducerService);
        MySQLBinlogService binlogService = new MySQLBinlogService(binlogClient, listener);

        // Start the service
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            binlogService.stop();
            kafkaProducerService.close();
        }));

        binlogService.start();
        System.out.println("MySQL Binlog to Kafka service is running. Press Ctrl+C to stop.");
    }

}
