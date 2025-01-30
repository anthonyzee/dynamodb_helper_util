# DynamoDB Helper Utility

This Python module provides helper functions for interacting with AWS DynamoDB using the `boto3` library. It includes functions for listing tables, creating tables, adding items, querying items, scanning items, parsing query strings, and deleting items.

## Features
- List all DynamoDB tables.
- Create a new table.
- Add items to a table.
- Scan a table with optional filtering and projection expressions.
- Query a table using key conditions and optional filters.
- Parse a query string into a structured list of conditions.
- Delete an item from a table.

## Installation

Ensure you have `boto3` installed:

```sh
pip install boto3
```

## Usage

### Importing the Module

```python
import json
from decimal import Decimal
import boto3
from your_module_name import *  # Replace with the actual module name

# Initialize DynamoDB resource
session = boto3.Session()
dynamo_resource = session.resource('dynamodb')
```

### List Tables

```python
list_tables(dynamo_resource)
```

### Create a Table

```python
table_config = {
    "table_name": "Users",
    "keys": [
        {"name": "UserId", "key": "HASH", "type": "S"}
    ],
    "capacity": "small"
}

create_table(table_config, dynamo_resource)
```

### Add an Item

```python
table = dynamo_resource.Table("Users")
item = {"UserId": "123", "Name": "Alice", "Age": 30}
add_item(item, table)
```

### Query Items

```python
filter_expression = "UserId eq '123'"
condition_list = parse_query_string(filter_expression, [])
table_keys = get_table_keys("Users", {"Users": ["UserId"]})

data = query_item(table, condition_list, table_keys, projection_expression="UserId, Name")
print(json.dumps(data, indent=2))
```

### Scan Items

```python
scan_result = scan_items(table, projection_expression="UserId, Name")
print(json.dumps(scan_result, indent=2))
```

### Delete an Item

```python
key = {"UserId": "123"}
delete_item(key, table)
```

## Example Query

```python
filter = "UserId eq '123'"
condition_list = parse_query_string(filter, [])
odata_list = query_item(table, condition_list, table_keys)
print(json.dumps(odata_list, indent=2))
```

## Explanation
- **`parse_query_string(query_string, condition_list)`**: Converts a string filter into a structured condition list.
- **`query_item(table, query_conditions, key_object, projection_expression)`**: Queries a table based on conditions.
- **`scan_items(table, query_params, projection_expression)`**: Scans a table with optional filtering and projection.
- **`delete_item(key, table)`**: Deletes an item from the table.

## Notes
- Ensure the AWS credentials and permissions are configured correctly to access DynamoDB.
- The **projection expression** allows retrieving only specific attributes, optimizing query performance.

## License
This project is licensed under the MIT License.

