package io.kkay.mysql.binlogkafka.listener;

import io.kkay.mysql.binlogkafka.producer.KafkaProducerService;

import static io.kkay.mysql.binlogkafka.serialize.TransactionSerializer.serializeTransactionEvents;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinlogEventListener {

	private final KafkaProducerService kafkaProducerService;
	private List<Event> transactionEvents;
	private Map<Long,TableMapEventData> tableMapEventDataMap;
	private boolean inTransaction;

	public BinlogEventListener(KafkaProducerService kafkaProducerService) {
		this.kafkaProducerService = kafkaProducerService;
		this.transactionEvents = new ArrayList<>();
		this.tableMapEventDataMap = new HashMap <>();
		this.inTransaction = false;
	}

	public void onEvent(Event event) {
		EventData data = event.getData();
		if (data != null) {
			if (data instanceof QueryEventData) {
				QueryEventData queryEventData = (QueryEventData) data;
				if ("BEGIN".equals(queryEventData.getSql())) {
					handleTransactionBegin();
				}
			} else if (data instanceof XidEventData) {
				handleTransactionCommit();
			} else if
			(data instanceof TableMapEventData){
				tableMapEventDataMap.put(((TableMapEventData) data).getTableId(), (TableMapEventData) data);
			}
			else if (inTransaction) {
				transactionEvents.add(event);
			} else {
				String eventJson = serializeEvent(data);
				kafkaProducerService.sendMessage(eventJson);
			}
		}
	}

	private void handleTransactionBegin() {
		inTransaction = true;
		transactionEvents.clear();
		System.out.println("Transaction started");
	}

	private void handleTransactionCommit() {
		inTransaction = false;
		String transactionJson = serializeTransactionEvents(transactionEvents,tableMapEventDataMap);
		kafkaProducerService.sendMessage(transactionJson);
		transactionEvents.clear();
		tableMapEventDataMap.clear();
		System.out.println("Transaction committed");
	}

	private String serializeEvent(EventData data) {
		return data.toString();
	}


}