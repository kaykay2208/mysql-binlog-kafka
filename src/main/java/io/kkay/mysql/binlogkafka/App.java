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
        String mysqlHost = "localhost";
        int mysqlPort = 3306;
        String mysqlUser = "root";
        String mysqlPassword = "";
        String kafkaBootstrapServers = "localhost:9092";
        String kafkaTopic = "mysql-binlog-topic";


        KafkaProducer<String, String> kafkaProducer = KafkaConfig.createKafkaProducer(kafkaBootstrapServers);
        KafkaProducerService kafkaProducerService = new KafkaProducerService(kafkaProducer, kafkaTopic);


        BinaryLogClient binlogClient = MySQLBinlogConfig.createBinlogClient(mysqlHost, mysqlPort, mysqlUser, mysqlPassword);
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
