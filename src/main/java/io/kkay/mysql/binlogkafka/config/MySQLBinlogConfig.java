package io.kkay.mysql.binlogkafka.config;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

public class MySQLBinlogConfig {

	public static BinaryLogClient createBinlogClient(String host, int port, String user, String password) {
		return new BinaryLogClient(host, port, user, password);
	}
}

