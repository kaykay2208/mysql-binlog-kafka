package io.kkay.mysql.binlogkafka;


import io.kkay.mysql.binlogkafka.config.KafkaConfig;
import io.kkay.mysql.binlogkafka.config.MySQLBinlogConfig;
import io.kkay.mysql.binlogkafka.listener.BinlogEventListener;
import io.kkay.mysql.binlogkafka.producer.KafkaProducerService;
import io.kkay.mysql.binlogkafka.service.MySQLBinlogService;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

public class App {


    public static final String MYSQL_HOST;
    public static final String MYSQL_PORT;
    public static final String MYSQL_USER;
    public static final String MYSQL_PASSWORD;
    public static final String KAFKA_BOOTSTRAP_SERVERS;
    public static final String KAFKA_TOPIC;


    static {
        MYSQL_HOST = System.getenv("MYSQL_HOST") != null ? System.getenv("MYSQL_HOST") : "localhost";
        MYSQL_PORT = System.getenv("MYSQL_PORT") != null ? System.getenv("MYSQL_PORT") : "3306";
        MYSQL_USER = System.getenv("MYSQL_USER") != null ? System.getenv("MYSQL_USER") : "root";
        MYSQL_PASSWORD = System.getenv("MYSQL_PASSWORD") != null ? System.getenv("MYSQL_PASSWORD") : "";
        KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092";
        KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? System.getenv("KAFKA_TOPIC") : "mysql-binlog-topic";
    }
    public static void main(String[] args) {



        KafkaProducer<String, String> kafkaProducer = KafkaConfig.createKafkaProducer(KAFKA_BOOTSTRAP_SERVERS);
        KafkaProducerService kafkaProducerService = new KafkaProducerService(kafkaProducer, KAFKA_TOPIC);


        BinaryLogClient binlogClient = MySQLBinlogConfig.createBinlogClient(MYSQL_HOST, Integer.valueOf(MYSQL_PORT), MYSQL_USER, MYSQL_PASSWORD);
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
