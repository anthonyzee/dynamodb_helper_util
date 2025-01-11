import json
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling Decimal objects."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


def list_tables(dynamo_resource):
    """List all DynamoDB tables."""
    print(list(dynamo_resource.tables.all()))


def create_table(table_config, dynamo_resource):
    """
    Create a DynamoDB table.
    Args:
        table_config (dict): Configuration for the table.
        dynamo_resource: Boto3 DynamoDB resource.
    """
    key_schema = [
        {'AttributeName': key['name'], 'KeyType': key['key']}
        for key in table_config['keys']
    ]
    attribute_definitions = [
        {'AttributeName': key['name'], 'AttributeType': key['type']}
        for key in table_config['keys']
    ]
    capacity_units = {
        "small": {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    }

    table = dynamo_resource.create_table(
        TableName=table_config['table_name'],
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput=capacity_units[table_config['capacity']]
    )

    print(table)


def add_item(item, table):
    """Add an item to a DynamoDB table."""
    return table.put_item(Item=item)


def scan_items(table, query_params=None):
    """
    Scan a DynamoDB table.
    Args:
        table: DynamoDB table object.
        query_params: Optional query parameters for filtering.

    Returns:
        dict: Scan results in OData format.
    """
    items = []
    if not query_params:
        response = table.scan()
        items.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
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

        response = table.scan(
            FilterExpression=filter_expression,
            ExpressionAttributeValues=expression_attr_values,
            ExpressionAttributeNames=expression_attr_names
        )
        items.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_attr_values,
                ExpressionAttributeNames=expression_attr_names,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])

    return {
        "d": {
            "__count": len(items),
            "results": items
        }
    }


def parse_query_string(query_string, condition_list):
    """
    Parse a query string into a condition list.
    Args:
        query_string (str): URL query string to parse.
        condition_list (list): List to store parsed conditions.

    Returns:
        list: Parsed condition list.
    """
    #field_1 eq 'field_1' and field_2 eq 'field_2/1.json'
    #(FirstName ne 'Mary' and LastName ne 'White') and UserName ne 'marywhite'

    # initialize variable
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

    for sQueryString in sQueryList:

        if sExpectedElement == 'condition_field':
            oConditionObject['condition_field'] = sQueryString
            sExpectedElement = 'condition_op'
        elif sExpectedElement == 'condition_op':
            if sQueryString == 'eq':
                oConditionObject['condition_op'] = '='
            elif sQueryString == 'gt':
                oConditionObject['condition_op'] = '>'
            elif sQueryString == 'ge':
                oConditionObject['condition_op'] = '>='
            elif sQueryString == 'lt':
                oConditionObject['condition_op'] = '<'
            elif sQueryString == 'le':
                oConditionObject['condition_op'] = '<='

            sExpectedElement = 'condition_value'
        elif sExpectedElement == 'condition_value':
            
            if sQueryString[0] == '\'':
                bStartValueQuote = True
                sJoinValue = sQueryString
            
            if sQueryString[-1] == '\'':
                bStartValueQuote = False
                oConditionObject['condition_value'] = sJoinValue
                sJoinValue = ""
                sExpectedElement = 'next_condition_logic'
            elif bStartValueQuote == False:
                oConditionObject['condition_value'] = sQueryString
                sExpectedElement = 'next_condition_logic'
            
        elif sExpectedElement == 'next_condition_logic':
            if sQueryString == 'and' or sQueryString == 'or':
                oNewConditionList.append(oConditionObject)
                oConditionObject = {
                    "condition_field": "",
                    "condition_value": "",
                    "condition_op": "",
                    "next_condition_logic": sQueryString
                }
                sExpectedElement = 'condition_field'
    
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

def query_item(table, query_conditions, key_object):
    """
    Query or scan a DynamoDB table based on conditions.
    
    Args:
        table: DynamoDB table object.
        query_conditions: List of condition objects for filtering.
        key_object: Key schema of the table to distinguish key attributes.

    Returns:
        dict: Query or scan results in OData format.
    """
    expression_values = {}
    expression_names = {}
    key_condition = ""
    filter_condition = ""

    # Build expressions for key and filter conditions
    for condition in query_conditions:
        field = condition["condition_field"]
        value = (
            condition["condition_value"][1:-1]  # Remove quotes for string values
            if condition["condition_value"][0] == "'"
            else int(condition["condition_value"][1:-1])  # Convert numeric values
        )
        expression_values[f":{field}"] = value
        expression_names[f"#{field}"] = field

        condition_expression = (
            f"#{field} {condition['condition_op']} :{field}"
        )

        if is_key(field, key_object):
            key_condition = (
                f"{key_condition} {condition['next_condition_logic']} {condition_expression}"
                if key_condition
                else condition_expression
            )
        else:
            filter_condition = (
                f"{filter_condition} {condition['next_condition_logic']} {condition_expression}"
                if filter_condition
                else condition_expression
            )

    # Determine whether to query or scan
    items = []
    if key_condition and filter_condition:
        # Query with both key and filter conditions
        response = table.query(
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names,
            KeyConditionExpression=key_condition,
            FilterExpression=filter_condition
        )
        items = collect_all_items(response, table, expression_values, expression_names, key_condition, filter_condition)
    elif not key_condition and filter_condition:
        # Scan with filter condition only
        response = table.scan(
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names,
            FilterExpression=filter_condition
        )
        items = collect_all_items(response, table, expression_values, expression_names, filter_expression=filter_condition)
    else:
        # Query with key condition only
        response = table.query(
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names,
            KeyConditionExpression=key_condition
        )
        items = collect_all_items(response, table, expression_values, expression_names, key_condition)

    # Format results in OData format
    return {
        "d": {
            "__count": len(items),
            "results": items
        }
    }


def collect_all_items(initial_response, table, expression_values, expression_names, key_condition=None, filter_expression=None):
    """
    Collect all items from paginated responses for a query or scan operation.
    
    Args:
        initial_response: Initial response from query or scan.
        table: DynamoDB table object.
        expression_values: Attribute values used in the query or scan.
        expression_names: Attribute names used in the query or scan.
        key_condition: Optional key condition expression.
        filter_expression: Optional filter condition expression.

    Returns:
        list: Aggregated list of items.
    """
    items = initial_response['Items']
    while 'LastEvaluatedKey' in initial_response:
        kwargs = {
            "ExpressionAttributeValues": expression_values,
            "ExpressionAttributeNames": expression_names,
            "ExclusiveStartKey": initial_response['LastEvaluatedKey']
        }
        if key_condition:
            kwargs["KeyConditionExpression"] = key_condition
        if filter_expression:
            kwargs["FilterExpression"] = filter_expression

        initial_response = table.query(**kwargs) if key_condition else table.scan(**kwargs)
        items.extend(initial_response['Items'])

    return items


def delete_item(key, table):
    """Delete an item from a DynamoDB table."""
    return table.delete_item(Key=key)
