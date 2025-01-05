package io.kkay.mysql.binlogkafka.service;

import io.kkay.mysql.binlogkafka.listener.BinlogEventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;

public class MySQLBinlogService {

	private final BinaryLogClient binlogClient;
	private final BinlogEventListener listener;

	public MySQLBinlogService(BinaryLogClient binlogClient, BinlogEventListener listener) {
		this.binlogClient = binlogClient;
		this.listener = listener;
	}

	public void start() {
		binlogClient.registerEventListener(listener::onEvent);
		new Thread(() -> {
			try {
				binlogClient.connect();
			} catch (IOException e) {
				System.err.println("Failed to connect to MySQL binlog: " + e.getMessage());
			}
		}).start();
	}

	public void stop() {
		try {
			binlogClient.disconnect();
		} catch (IOException e) {
			System.err.println("Failed to disconnect from MySQL binlog: " + e.getMessage());
		}
	}
}

