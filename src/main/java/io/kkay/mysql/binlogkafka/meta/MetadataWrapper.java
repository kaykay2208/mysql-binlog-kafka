package io.kkay.mysql.binlogkafka.meta;

public class MetadataWrapper {
	private String ip;
	private String schema;
	private String tableName;
	private TableMetadata tableMetadata;

	// Getters and setters
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public TableMetadata getTableMetadata() {
		return tableMetadata;
	}

	public void setTableMetadata(TableMetadata tableMetadata) {
		this.tableMetadata = tableMetadata;
	}
}

