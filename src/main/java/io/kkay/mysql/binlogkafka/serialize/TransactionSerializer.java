package io.kkay.mysql.binlogkafka.serialize;

import io.kkay.mysql.binlogkafka.meta.TableMetadata;

import static io.kkay.mysql.binlogkafka.App.MYSQL_HOST;
import static io.kkay.mysql.binlogkafka.meta.MySQLBinlogMetadataFetcher.getMetadata;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class TransactionSerializer
{
	public static String serializeTransactionEvents (List <Event> transactionEvents,Map<Long,TableMapEventData> tableMeta)
	{

		JSONObject json = new JSONObject();
		String schema=tableMeta.entrySet().stream().findFirst().get().getValue().getDatabase();
		json.put("schema" , schema);
		for(Event event : transactionEvents)
		{
			JSONObject eventJson = serializeEvent(event , tableMeta,schema);
			if(!json.has(String.valueOf(event.getHeader().getTimestamp())))
			{
				JSONArray events = new JSONArray().put(eventJson);
				json.put(String.valueOf(event.getHeader().getTimestamp()) , events);
			}
			else
			{
				json.getJSONArray(String.valueOf(event.getHeader().getTimestamp())).put(eventJson);
			}
		}
		return json.toString();
	}

	private static JSONObject serializeEvent (Event event , Map <Long, TableMapEventData> tableMeta , String schema)
	{
		JSONObject json = new JSONObject();

		EventData data = event.getData();

		if(data instanceof WriteRowsEventData)
		{
			json.put(Constants.OPERATION , Constants.CREATE);
			String table = tableMeta.get(((WriteRowsEventData) event.getData()).getTableId()).getTable();
			TableMetadata metadata = getMetadata(MYSQL_HOST , schema , table);
			json.put("table" , table);
			JSONArray rows = new JSONArray();
			for(Serializable[] row : ((WriteRowsEventData) data).getRows())
			{
				JSONObject values = new JSONObject();
				JSONObject datas = new JSONObject();
				BitSet affectedColumns = ((WriteRowsEventData) data).getIncludedColumns();
				List <Integer> indexes = affectedColumns.stream().boxed().collect(Collectors.toList());
				for(Integer index : indexes)
				{
					values.put(metadata.getColumnName(index) , translate(row[index]));
				}
				datas.put("v" , values);
				JSONObject pk = new JSONObject();
				for(Integer pk1 : metadata.getPrimaryKeyIndexes())
				{
					pk.put(metadata.getColumnName(pk1) , translate(row[pk1]));
				}
				datas.put("pk" , pk);
				rows.put(datas);
			}
			json.put("rows" , rows);
		}
		else if(data instanceof UpdateRowsEventData)
		{
			json.put(Constants.OPERATION , Constants.UPDATE);
			String table = tableMeta.get(((UpdateRowsEventData) event.getData()).getTableId()).getTable();
			TableMetadata metadata = getMetadata(MYSQL_HOST , schema , table);
			json.put("table" , table);
			JSONObject datas = new JSONObject();
			JSONArray rows = new JSONArray();
			for(Map.Entry <Serializable[], Serializable[]> row : ((UpdateRowsEventData) data).getRows())
			{
				JSONObject values = new JSONObject();
				JSONObject oldValues = new JSONObject();
				BitSet affectedColumns = ((UpdateRowsEventData) data).getIncludedColumns();
				List <Integer> indexes = affectedColumns.stream().boxed().collect(Collectors.toList());
				for(Integer index : indexes)
				{
					String oldval = translate(row.getKey()[index]);
					String newval = translate(row.getValue()[index]);
					if((oldval == null && newval == null) || (oldval != null && oldval.equals(newval)))
					{
						continue;
					}
					values.put(metadata.getColumnName(index) , newval);
					oldValues.put(metadata.getColumnName(index) , oldval);
				}
				datas.put("v" , values);
				datas.put("ov" , oldValues);
				JSONObject pk = new JSONObject();
				for(Integer pk1 : metadata.getPrimaryKeyIndexes())
				{
					pk.put(metadata.getColumnName(pk1) , translate(row.getKey()[pk1]));
				}
				datas.put("pk" , pk);
				rows.put(datas);
			}
			json.put("rows" , rows);
		}
		else
		{
			json.put(Constants.OPERATION , Constants.DELETE);
			String table = tableMeta.get(((DeleteRowsEventData) event.getData()).getTableId()).getTable();
			TableMetadata metadata = getMetadata(MYSQL_HOST, schema , table);
			json.put("table" , table);
			JSONArray rows = new JSONArray();
			for(Serializable[] row : ((DeleteRowsEventData) data).getRows())
			{
				JSONObject values = new JSONObject();
				JSONObject datas = new JSONObject();
				BitSet affectedColumns = ((DeleteRowsEventData) data).getIncludedColumns();
				List <Integer> indexes = affectedColumns.stream().boxed().collect(Collectors.toList());
				for(Integer index : indexes)
				{
					values.put(String.valueOf(index) , translate(row[index]));
				}
				JSONObject pk = new JSONObject();
				for(Integer pk1 : metadata.getPrimaryKeyIndexes())
				{
					pk.put(metadata.getColumnName(pk1) , translate(row[pk1]));
				}
				datas.put("pk" , pk);
				datas.put("ov" , values);
				rows.put(datas);
			}
			json.put("rows" , rows);
		}

		return json;
	}

	private static String timestamp2string (long time)
	{
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(time));
	}

	private static String translate (Serializable c)
	{
		if(c == null)
		{
			return null;
		}
		else if(c instanceof byte[])
		{
			return new String((byte[]) c , StandardCharsets.UTF_8);
		}
		else if(c instanceof Timestamp)
		{
			return (((Timestamp) c).getTime())+"";
		}
		else if(c instanceof Date)
		{
			return (((Date) c).getTime())+"";
		}
		return c.toString();
	}
}
