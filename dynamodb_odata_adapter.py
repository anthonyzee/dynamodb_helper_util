import json
import re
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr, And, Or

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling Decimal objects."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

def parse_query_string(query_string, condition_list):
    """
    Parse a query string into a condition list.
    Supports: eq, gt, ge, lt, le, startswith(field,'value'), endswith(field,'value'), 
              substringof('value',field), contains(field,'value')
    """
    sExpectedElement = 'condition_field'
    bStartValueQuote = False
    sJoinValue = ""
    sQueryList = query_string.split(' ')
    oNewConditionList = condition_list
    oConditionObject = {
        "condition_field": "",
        "condition_value": "",
        "condition_op": "",
        "next_condition_logic": ""
    }

    i = 0
    while i < len(sQueryList):
        sQueryString = sQueryList[i]

        if sExpectedElement == 'condition_field':
            if sQueryString.startswith('startswith('):
                match = re.match(r"startswith\(([^,]+),\s*'([^']+)'\)", sQueryString)
                if match:
                    field, value = match.groups()
                    oConditionObject['condition_field'] = field
                    oConditionObject['condition_op'] = 'startswith'
                    oConditionObject['condition_value'] = f"'{value}'"
                    sExpectedElement = 'next_condition_logic'
                else:
                    raise ValueError(f"Invalid startswith syntax: {sQueryString}")

            elif sQueryString.startswith('endswith('):
                match = re.match(r"endswith\(([^,]+),\s*'([^']+)'\)", sQueryString)
                if match:
                    field, value = match.groups()
                    oConditionObject['condition_field'] = field
                    oConditionObject['condition_op'] = 'endswith'
                    oConditionObject['condition_value'] = f"'{value}'"
                    sExpectedElement = 'next_condition_logic'
                else:
                    raise ValueError(f"Invalid endswith syntax: {sQueryString}")

            elif sQueryString.startswith('contains('):
                match = re.match(r"contains\(([^,]+),\s*'([^']+)'\)", sQueryString)
                if match:
                    field, value = match.groups()
                    oConditionObject['condition_field'] = field
                    oConditionObject['condition_op'] = 'contains'
                    oConditionObject['condition_value'] = f"'{value}'"
                    sExpectedElement = 'next_condition_logic'
                else:
                    raise ValueError(f"Invalid contains syntax: {sQueryString}")

            elif sQueryString.startswith('substringof('):
                match = re.match(r"substringof\(\s*'([^']+)'\s*,\s*([^)]+)\)", sQueryString)
                if match:
                    value, field = match.groups()
                    oConditionObject['condition_field'] = field
                    oConditionObject['condition_op'] = 'substringof'
                    oConditionObject['condition_value'] = f"'{value}'"
                    sExpectedElement = 'next_condition_logic'
                else:
                    raise ValueError(f"Invalid substringof syntax: {sQueryString}")

            else:
                oConditionObject['condition_field'] = sQueryString
                sExpectedElement = 'condition_op'

        elif sExpectedElement == 'condition_op':
            op_map = {
                'eq': '=',
                'gt': '>',
                'ge': '>=',
                'lt': '<',
                'le': '<='
            }
            if sQueryString in op_map:
                oConditionObject['condition_op'] = op_map[sQueryString]
                sExpectedElement = 'condition_value'
            else:
                raise ValueError(f"Unsupported operator: {sQueryString}")

        elif sExpectedElement == 'condition_value':
            if not bStartValueQuote:
                if sQueryString.startswith("'") and not sQueryString.endswith("'"):
                    bStartValueQuote = True
                    sJoinValue = sQueryString
                elif sQueryString.startswith("'") and sQueryString.endswith("'"):
                    # Single word quoted value
                    oConditionObject['condition_value'] = sQueryString
                    sExpectedElement = 'next_condition_logic'
                else:
                    # Unquoted value
                    oConditionObject['condition_value'] = sQueryString
                    sExpectedElement = 'next_condition_logic'
            else:
                sJoinValue += ' ' + sQueryString
                if sQueryString.endswith("'"):
                    bStartValueQuote = False
                    oConditionObject['condition_value'] = sJoinValue
                    sJoinValue = ""
                    sExpectedElement = 'next_condition_logic'

        elif sExpectedElement == 'next_condition_logic':
            if sQueryString in ['and', 'or']:
                oConditionObject['next_condition_logic'] = sQueryString
                oNewConditionList.append(oConditionObject)
                oConditionObject = {
                    "condition_field": "",
                    "condition_value": "",
                    "condition_op": "",
                    "next_condition_logic": ""
                }
                sExpectedElement = 'condition_field'
            else:
                # End of query string or malformed connector
                pass

        i += 1

    if oConditionObject['condition_field']:  # Avoid appending empty
        oNewConditionList.append(oConditionObject)

    return oNewConditionList

def get_table_keys(table_name, table_key_mapping):
    """
    Retrieves the key fields for a given table name from a mapping object.

    Args:
        table_name (str): The name of the table to look up.
        table_key_mapping (dict): A dictionary where the keys are table names and the values 
                                  are lists of key field names for each table.
    
    Returns:
        list: A list of key field names for the specified table, or None if the table name is not found.
    
    Example:
        table_key_mapping = {
            "table_name_1": ["field_1", "field_2"],
            "table_name_2": ["field_1", "field_2"]
        }

        get_table_keys("table_name_1", table_key_mapping)
        # Output: ["field_1", "field_2"]

        get_table_keys("non_existent_table", table_key_mapping)
        # Output: None
    """
    return table_key_mapping.get(table_name)

def is_key(field_name, key_object):
    """
    Check if a field is a key attribute.

    Args:
        field_name (str): The name of the field to check.
        key_object (dict or None): The object containing key attributes.

    Returns:
        bool: True if the field is a key or if key_object is None, otherwise False.
    """
    return key_object is None or field_name in key_object

def scan_items(table, query_params=None, projection_expression=None):
    """
    Scan a DynamoDB table.
    Args:
        table: DynamoDB table object.
        query_params: Optional query parameters for filtering.
        projection_expression: Optional projection expression.

    Returns:
        dict: Scan results in OData format.
    """
    items = []
    scan_kwargs = {}

    if projection_expression:
        scan_kwargs["ProjectionExpression"] = projection_expression

    if not query_params:
        response = table.scan(**scan_kwargs)
        items.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **scan_kwargs)
            items.extend(response['Items'])
    else:
        expression_attr_values = {}
        expression_attr_names = {}
        filter_expression = ""

        for condition in query_params:
            condition_value = (
                int(condition['condition_value'][1:-1])
                if condition['condition_value'][0] != '\''
                else condition['condition_value'][1:-1]
            )

            expression_attr_values[f":{condition['condition_field']}"] = condition_value
            expression_attr_names[f"#{condition['condition_field']}"] = condition['condition_field']

            filter_condition = (
                f"#{condition['condition_field']} {condition['condition_op']} "
                f":{condition['condition_field']}"
            )

            if filter_expression:
                filter_expression += f" {condition['next_condition_logic']} {filter_condition}"
            else:
                filter_expression = filter_condition

        scan_kwargs.update({
            "FilterExpression": filter_expression,
            "ExpressionAttributeValues": expression_attr_values,
            "ExpressionAttributeNames": expression_attr_names,
        })

        response = table.scan(**scan_kwargs)
        items.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **scan_kwargs)
            items.extend(response['Items'])

    return {
        "d": {
            "__count": len(items),
            "results": items
        }
    }

def normalize_attr_name(field, expression_names):
    """
    Normalize a dotted field like 'uds.username' into a DynamoDB expression attribute name like '#uds_username',
    and populate ExpressionAttributeNames accordingly.
    """
    alias = f"#{field.replace('.', '_')}"
    expression_names[alias] = field
    return alias

def query_item(table, query_conditions, key_object, projection_expression=None):
    key_condition_parts = []
    key_condition = None
    filter_conditions = []
    expression_names = {}
    expression_values = {}
    use_expression_names = False

    for condition in query_conditions:
        field = condition["condition_field"]
        op = condition["condition_op"]
        raw_value = condition["condition_value"]
        logic = condition.get("next_condition_logic", "").lower()

        # Parse value
        value = raw_value[1:-1] if raw_value.startswith("'") else int(raw_value)

        # Normalize symbolic operators to names
        op = {
            '>': 'gt',
            '>=': 'ge',
            '<': 'lt',
            '<=': 'le',
            '=': '='
        }.get(op, op)

        # Normalize field name
        if '.' in field:
            dyn_field = normalize_attr_name(field, expression_names)
            value_alias = f":{field.replace('.', '_')}"               # creates :uds_username
            expression_values[value_alias] = value                    # assigns value to :uds_username
            use_expression_names = True            
        else:
            dyn_field = field
            value_alias = f":{field}"
            expression_values[value_alias] = value

        expr = None

        if is_key(field, key_object):
            if logic == 'or':
                raise ValueError("DynamoDB KeyConditionExpression does not support OR")

            key_op_map = {
                '=': lambda f, v: Key(f).eq(v),
                'gt': lambda f, v: Key(f).gt(v),
                'ge': lambda f, v: Key(f).gte(v),
                'lt': lambda f, v: Key(f).lt(v),
                'le': lambda f, v: Key(f).lte(v),
                'startswith': lambda f, v: Key(f).begins_with(v)
            }

            if use_expression_names:
                print(f"Key condition: {dyn_field} {op} {value_alias}")
                expr = key_op_map[op](dyn_field, value_alias)
                key_condition = expr if key_condition is None else And(key_condition, expr)

                if op == '=':
                    key_condition_parts.append(f"{dyn_field} = {value_alias}")
                elif op == 'startswith':
                    key_condition_parts.append(f"begins_with({dyn_field}, {value_alias})")
                else:
                    raise ValueError("Unsupported KeyConditionExpression op: " + op)
            else:
                expr = key_op_map[op](dyn_field, value)
                key_condition = expr if key_condition is None else And(key_condition, expr)

        else:
            attr_op_map = {
                '=': lambda f, v: Attr(f).eq(v),
                'gt': lambda f, v: Attr(f).gt(v),
                'ge': lambda f, v: Attr(f).gte(v),
                'lt': lambda f, v: Attr(f).lt(v),
                'le': lambda f, v: Attr(f).lte(v),
                'startswith': lambda f, v: Attr(f).begins_with(v),
                'contains': lambda f, v: Attr(f).contains(v),
                'substringof': lambda f, v: Attr(f).contains(v),
                'endswith': lambda f, v: Attr(f).contains(v)
            }

            if op in attr_op_map:
                if use_expression_names:
                    print(f"Field condition1: {dyn_field} {op} {value_alias}")
                else:
                    print(f"Field condition2: {dyn_field} {op} {value}")
                    expr = attr_op_map[op](dyn_field, value)
                    filter_conditions.append((expr, logic))

    # Combine filter expressions
    filter_expr = None
    for i, (f, logic) in enumerate(filter_conditions):
        filter_expr = f if i == 0 else (And(filter_expr, f) if logic == 'and' else Or(filter_expr, f))

    query_kwargs = {}
    if projection_expression:
        query_kwargs["ProjectionExpression"] = projection_expression
    if use_expression_names:
        query_kwargs["ExpressionAttributeNames"] = expression_names
        query_kwargs["ExpressionAttributeValues"] = expression_values

    items = []

    if key_condition and filter_expr:
        print("Using both KeyConditionExpression and FilterExpression")
        query_kwargs.update({
            "KeyConditionExpression": key_condition,
            "FilterExpression": filter_expr,
        })
        response = table.query(**query_kwargs)
        items = collect_all_items(response, table, query_kwargs, key_condition, filter_expr)

    elif key_condition:
        print("Using KeyConditionExpression only")
        if use_expression_names:
            key_condition_expr = " AND ".join(key_condition_parts) if key_condition_parts else None
            query_kwargs.update({
                "KeyConditionExpression": key_condition_expr,
                "ExpressionAttributeNames": expression_names,
                "ExpressionAttributeValues": expression_values,
            })
        else:
            #query_kwargs["KeyConditionExpression"] = key_condition
            query_kwargs.update({
                "KeyConditionExpression": key_condition,
            })
        
        print(query_kwargs)
        response = table.query(**query_kwargs)
        items = collect_all_items(response, table, query_kwargs, key_condition)

    elif filter_expr:
        print("Using FilterExpression only")
        return scan_items(table, query_conditions)
    else:
        print("No KeyConditionExpression or FilterExpression, performing full scan")
        return scan_items(table)
    
    # Post-filter for endswith
    for condition in query_conditions:
        if condition["condition_op"] == "endswith":
            field = condition["condition_field"]
            value = condition["condition_value"][1:-1]
            items = [item for item in items if str(item.get(field, "")).endswith(value)]

    return {
        "d": {
            "__count": len(items),
            "results": items
        }
    }

def collect_all_items(initial_response, table, query_kwargs, key_condition=None, filter_expression=None, projection_expression=None):
    items = initial_response.get('Items', [])

    while 'LastEvaluatedKey' in initial_response:
        kwargs = {
            "ExclusiveStartKey": initial_response['LastEvaluatedKey'],
            **query_kwargs
        }

        if key_condition:
            kwargs["KeyConditionExpression"] = key_condition
        if filter_expression:
            kwargs["FilterExpression"] = filter_expression
        if projection_expression:
            kwargs["ProjectionExpression"] = projection_expression

        initial_response = table.query(**kwargs) if key_condition else table.scan(**kwargs)
        items.extend(initial_response.get('Items', []))

    return items
