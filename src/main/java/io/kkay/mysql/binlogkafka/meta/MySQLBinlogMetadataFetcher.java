package io.kkay.mysql.binlogkafka.meta;

import static io.kkay.mysql.binlogkafka.App.MYSQL_HOST;
import static io.kkay.mysql.binlogkafka.App.MYSQL_PASSWORD;
import static io.kkay.mysql.binlogkafka.App.MYSQL_PORT;
import static io.kkay.mysql.binlogkafka.App.MYSQL_USER;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLBinlogMetadataFetcher {

	// Map: IP -> Schema -> Table Name -> Metadata
	private static final Map<String, Map<String, Map<String, TableMetadata>>> metadataHierarchy = new HashMap<>();
	private static final String SCHEMA_DIRECTORY = System.getProperty("user.dir") + "/hostip";

	static {
		loadMetadata();
	}

	private static void loadMetadata() {
		ObjectMapper objectMapper = new ObjectMapper();
		File schemaDir = new File(SCHEMA_DIRECTORY);

		if (!schemaDir.exists() || !schemaDir.isDirectory()) {
			System.err.println("Schema directory not found: " + SCHEMA_DIRECTORY);
			return;
		}

		File[] jsonFiles = schemaDir.listFiles((dir, name) -> name.endsWith(".json"));
		if (jsonFiles == null || jsonFiles.length == 0) {
			System.err.println("No JSON metadata files found in: " + SCHEMA_DIRECTORY);
			return;
		}

		for (File file : jsonFiles) {
			try {
				MetadataWrapper wrapper = objectMapper.readValue(file, MetadataWrapper.class);

				metadataHierarchy
					.computeIfAbsent(wrapper.getIp(), ip -> new HashMap<>())
					.computeIfAbsent(wrapper.getSchema(), schema -> new HashMap<>())
					.put(wrapper.getTableName(), wrapper.getTableMetadata());

				System.out.println("Loaded metadata for table: " + wrapper.getTableName() +
					" (IP: " + wrapper.getIp() + ", Schema: " + wrapper.getSchema() + ")");
			} catch (IOException e) {
				System.err.println("Failed to load metadata from file: " + file.getName());
				e.printStackTrace();
			}
		}
	}

	public static TableMetadata getMetadata(String ip, String schema, String tableName) {
		Map<String, Map<String, TableMetadata>> schemaMap = metadataHierarchy.get(ip);
		if (schemaMap == null) {
			schemaMap = new HashMap<>();
			metadataHierarchy.put(ip, schemaMap);
		}

		Map<String, TableMetadata> tableMap = schemaMap.get(schema);
		if (tableMap == null) {
			tableMap = new HashMap<>();
			schemaMap.put(schema, tableMap);
		}

		TableMetadata metadata = tableMap.get(tableName);
		if (metadata == null) {
			saveMetadata(ip, schema, tableName);
			metadata = tableMap.get(tableName);
		}

		return metadata;
	}

	private static void saveMetadata(String hostIp, String database, String table) {
		String url = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + database
			+ "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
		String username =MYSQL_USER;
		String password = MYSQL_PASSWORD;


		Map<Integer,String> indexColumnMap = new HashMap();
		List<Integer> primaryKeys = new ArrayList();

		try (Connection connection = DriverManager.getConnection(url, username, password)) {
			Statement statement = connection.createStatement();


			String columnQuery = "SELECT ORDINAL_POSITION, COLUMN_NAME FROM information_schema.COLUMNS " +
				"WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "' " +
				"ORDER BY ORDINAL_POSITION";
			ResultSet columnResult = statement.executeQuery(columnQuery);
			while (columnResult.next()) {
				int index = columnResult.getInt("ORDINAL_POSITION")-1;
				String columnName = columnResult.getString("COLUMN_NAME");
				indexColumnMap.put(index, columnName);
			}


			String pkQuery = "SELECT ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE " +
				"WHERE TABLE_SCHEMA = '" + database + "' " +
				"AND TABLE_NAME = '" + table + "' " +
				"AND CONSTRAINT_NAME = 'PRIMARY'";
			ResultSet pkResult = statement.executeQuery(pkQuery);
			while (pkResult.next()) {
				primaryKeys.add(pkResult.getInt("ORDINAL_POSITION")-1);
			}




			TableMetadata tableMetadata = new TableMetadata();
			tableMetadata.setColumnNames(indexColumnMap);
			tableMetadata.setPrimaryKeyIndexes(primaryKeys);
			tableMetadata.setTableName(table);

			metadataHierarchy
				.computeIfAbsent(hostIp, ip -> new HashMap<>())
				.computeIfAbsent(database, schema -> new HashMap<>())
				.put(table, tableMetadata);

			MetadataWrapper metadataWrapper = new MetadataWrapper();
			metadataWrapper.setTableMetadata(tableMetadata);
			metadataWrapper.setIp(hostIp);
			metadataWrapper.setSchema(database);
			metadataWrapper.setTableName(table);


			//			metadata.put("database", database);
			//			metadata.put("table", table);
			//			metadata.put("indexToColumnMap", indexColumnMap);
			//			metadata.put("primaryKeys", primaryKeys);
			//
			//
			saveToFile(hostIp, database, table, metadataWrapper);

			System.out.println("Metadata saved for: " + database + "." + table);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void saveToFile (String hostIp , String database , String table , MetadataWrapper metadata)
	{
		File directory = new File(SCHEMA_DIRECTORY);
		if(!directory.exists())
		{
			directory.mkdirs();
		}

		File file = new File(SCHEMA_DIRECTORY + "/" + hostIp + "_" + database + "_" + table + ".json");
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		try
		{
			objectMapper.writeValue(file , metadata);
		}
		catch (IOException e)
		{
			System.err.println("Failed to save metadata to file: " + file.getName());
			e.printStackTrace();
		}
	}


	public static Map<String, Map<String, Map<String, TableMetadata>>> getAllMetadata() {
		return metadataHierarchy;
	}
}