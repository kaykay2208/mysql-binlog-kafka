
This project deals with capturing bin log of transactions, here the operations performed on entities are recorded. The events are stored with information such as timestamp, table name, operation type (create or update), and the values associated with the transaction.

## Data Structure

The  data is represented in a JSON format with the following structure:

```json
{
    "timestamp": [{
        "rows": [
            {
                "v": {
                    "columnName0": <new value 1>,
                    "columnName1": <new value 2>,
                    "columnName4": <new value 3>,
                    "columnName7": <new value 4>
                },
                "ov": {
                    "columnName0": <old value 1>,
                    "columnName1": <old value 2>,
                    "columnName4": <old value 3>,
                    "columnName7": <old value 4>
                }
            }
        ],
        "table": "<table_name>",
        "O": "<operation>"
    }],
    "schema":<schema_name>
}
