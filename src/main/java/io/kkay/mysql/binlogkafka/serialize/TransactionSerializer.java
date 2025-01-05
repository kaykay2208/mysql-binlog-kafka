package io.kkay.mysql.binlogkafka.serialize;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class TransactionSerializer
{
	public static String serializeTransactionEvents (List <Event> transactionEvents,Map<Long,TableMapEventData> tableMeta)
	{

		JSONObject json = new JSONObject();
		json.put("schema" , tableMeta.entrySet().stream().findFirst().get().getValue().getDatabase());
		for(Event event : transactionEvents)
		{
			JSONObject eventJson = serializeEvent(event , tableMeta);
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

	private static JSONObject serializeEvent (Event event , Map <Long, TableMapEventData> tableMeta)
	{
		JSONObject json = new JSONObject();

		EventData data = event.getData();
		if(data instanceof WriteRowsEventData)
		{
			json.put(Constants.OPERATION , Constants.CREATE);
			json.put("table", tableMeta.get(((WriteRowsEventData)event.getData()).getTableId()).getTable());
			JSONArray rows = new JSONArray();
			for(Object[] row : ((WriteRowsEventData) data).getRows())
			{
				JSONObject values = new JSONObject();
				JSONObject datas = new JSONObject();
				BitSet affectedColumns = ((WriteRowsEventData) data).getIncludedColumns();
				List <Integer> indexes = affectedColumns.stream().boxed().collect(Collectors.toList());
				for(Integer index : indexes)
				{
					values.put(String.valueOf(index) , row[index]);
				}
				datas.put("v", values);
				rows.put(datas);
			}
			json.put("rows",rows);
		}
		else if(data instanceof UpdateRowsEventData)
		{
			json.put(Constants.OPERATION , Constants.UPDATE);
			json.put("table", tableMeta.get(((UpdateRowsEventData)event.getData()).getTableId()).getTable());
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
					values.put(String.valueOf(index) , row.getValue()[index]);
					oldValues.put(String.valueOf(index) , row.getKey()[index]);
				}
				datas.put("v" , values);
				datas.put("ov" , oldValues);
				rows.put(datas);
			}
			json.put("rows",rows);
		}
		else
		{
			json.put(Constants.OPERATION , Constants.DELETE);
			json.put("table", tableMeta.get(((DeleteRowsEventData)event.getData()).getTableId()).getTable());
			JSONArray rows = new JSONArray();
			for(Object[] row : ((DeleteRowsEventData) data).getRows())
			{
				JSONObject values = new JSONObject();
				JSONObject datas = new JSONObject();
				BitSet affectedColumns = ((DeleteRowsEventData) data).getIncludedColumns();
				List <Integer> indexes = affectedColumns.stream().boxed().collect(Collectors.toList());
				for(Integer index : indexes)
				{
					values.put(String.valueOf(index) , row[index]);
				}
				datas.put("ov", values);
				rows.put(datas);
			}
			json.put("rows",rows);
		}

		return json;
	}
}
