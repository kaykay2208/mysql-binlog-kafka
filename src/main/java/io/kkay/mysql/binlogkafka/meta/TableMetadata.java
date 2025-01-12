package io.kkay.mysql.binlogkafka.meta;


import java.util.List;
import java.util.Map;

public class TableMetadata {
	private String tableName;



	private Map <Integer,String> columnNames;
	private List<Integer> primaryKeyIndexes;


	// Getters and setters
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName(int index) {
		return columnNames.get(index);
	}
	public Map <Integer, String> getColumnNames ()
	{
		return columnNames;
	}

	public void setColumnNames (Map <Integer, String> columnNames)
	{
		this.columnNames = columnNames;
	}


	public List<Integer> getPrimaryKeyIndexes() {
		return primaryKeyIndexes;
	}

	public void setPrimaryKeyIndexes(List<Integer> primaryKeyIndexes) {
		this.primaryKeyIndexes = primaryKeyIndexes;
	}

	@Override
	public String toString() {
		return "TableMetadata{" +
			"tableName='" + tableName + '\'' +
			", columnNames=" + columnNames +
			", primaryKeyIndexes=" + primaryKeyIndexes +
			'}';
	}
}
