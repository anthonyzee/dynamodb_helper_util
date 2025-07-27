# DynamoDB OData Adapter

**A Python utility to query DynamoDB using OData-style query strings.**

This script enables translating frontend query formats (like `$filter=user_id eq 'U123'`) into backend DynamoDB queries seamlessly using `boto3`.

---

## 🚀 Core Feature

### `execute_dynamodb_query_from_odata(table_object, query_string, key_object, projection_expression=None)`

This is the main entry point for executing OData-style queries against a DynamoDB table.

#### ✅ Parameters:
- `table_object`: A `boto3` DynamoDB Table resource.
- `query_string`: An OData-like filter string, e.g. `$filter=user_id eq 'U123' and is_active eq true`
- `key_object`: A dictionary defining key fields (e.g., `{ "user_id": "hash" }`).
- `projection_expression`: Optional string to project only certain fields.

#### 🔄 Returns:
A dictionary in the following OData-style format:
```json
{
  "d": {
    "__count": 3,
    "results": [ ...items... ]
  }
}
```

---

## 💡 Example

```python
import boto3
from your_script_name import execute_dynamodb_query_from_odata

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('YourTableName')

query_string = "$filter=user_id eq 'U123' and is_active eq true"
key_fields = { "user_id": "hash" }

results = execute_dynamodb_query_from_odata(table, query_string, key_fields)

print(results["d"]["results"])
```

---

## 🔍 Supported OData Syntax

| OData Expression         | Supported |
|--------------------------|-----------|
| `field eq 'value'`       | ✅        |
| `field gt 100`           | ✅        |
| `startswith(field,'abc')`| ✅        |
| `endswith(field,'xyz')`  | ✅ (post-filter) |
| `contains(field,'val')`  | ✅        |
| `substringof('val',field)`| ✅      |
| `and`, `or`              | ✅        |

---

## 📜 License

MIT License

---

## 🙌 Contributions

Contributions welcome! Open a PR or create an issue if you'd like to enhance support for other OData operations or extend functionality.
