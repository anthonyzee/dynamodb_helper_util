# dynamodb_odata_adapter

**A lightweight Python utility that translates OData-style query strings into DynamoDB query parameters.**

This utility simplifies handling frontend query filters (like `$filter=field eq 'value'`) and converts them into expressions compatible with AWS DynamoDB's `query()` and `scan()` methods.

---

## 🚀 Features

- Converts OData-style query strings into DynamoDB-compatible key and filter expressions.
- Supports logical operators (`and`, `or`) and comparison operators (`eq`, `ne`, `gt`, `lt`, `ge`, `le`).
- Handles nested attributes using dot notation (`field.subfield`).
- Generates:
  - `KeyConditionExpression`
  - `FilterExpression`
  - `ExpressionAttributeNames`
  - `ExpressionAttributeValues`

---

## 📦 Installation

```bash
pip install urllib3 boto3
```

Copy `dynamodb_odata_adapter.py` into your project directory.

---

## 🧠 Usage

```python
from dynamodb_odata_adapter import parse_odata_query

query_string = "$filter=pfy_com_id eq 'PFY001' and pfy_prd_var_id eq 'PRD001'"
key_object = {"pfy_com_id": "hash", "pfy_prd_var_id": "range"}

query_params = parse_odata_query(query_string, key_object)

# Result:
# {
#     'KeyConditionExpression': ...,
#     'FilterExpression': ...,
#     'ExpressionAttributeNames': {...},
#     'ExpressionAttributeValues': {...}
# }
```

You can use the resulting `query_params` with `table.query()` or `table.scan()` from `boto3`.

---

## 🧪 Example with DynamoDB

```python
import boto3
from dynamodb_odata_adapter import parse_odata_query

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('YourTableName')

query_string = "$filter=user_id eq 'U123' and is_active eq true"
key_object = {"user_id": "hash"}

params = parse_odata_query(query_string, key_object)

response = table.query(**params)
```

---

## 🛠 Supported OData Operators

| OData Operator | DynamoDB Equivalent |
|----------------|---------------------|
| eq             | =                   |
| ne             | <>                  |
| gt             | >                   |
| ge             | >=                  |
| lt             | <                   |
| le             | <=                  |
| and            | AND                 |
| or             | OR                  |

---

## 📁 File Structure

```
dynamodb_odata_adapter.py
README.md
```

---

## 📜 License

MIT License

---

## 🙌 Contributing

PRs and suggestions welcome! Feel free to fork and enhance for your own use cases.
