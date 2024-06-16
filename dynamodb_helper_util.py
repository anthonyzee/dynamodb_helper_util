from decimal import Decimal
import json 

def listTable(oDynResource):
    print(list(oDynResource.tables.all()))

def createTable(oTableObject, oDynResource):
    # oTableObject = {"table_name":"test", "capacity": "small", "keys": [{"name":"field_1", "key":"HASH", "type":"S"}]}
    oKeySchemaList = []
    oAttributeList = []
    oCapacityObject = {
        "small":{
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    }

    for oKeyObject in oTableObject['keys']:
        oKeySchemaObject = {
            'AttributeName': oKeyObject['name'],
            'KeyType': oKeyObject['key']            
        }
        oKeySchemaList.append(oKeySchemaObject)
        oAttributeObject = {
            'AttributeName': oKeyObject['name'],
            'AttributeType': oKeyObject['type']            
        }
        oAttributeList.append(oAttributeObject)

    oTableObject = oDynResource.create_table(
        TableName=oTableObject['table_name'],
        KeySchema=oKeySchemaList,
        AttributeDefinitions=oAttributeList,
        ProvisionedThroughput=oCapacityObject[oTableObject['capacity']]
    )

    print(oTableObject)

def addItem(oNewItemObject, oTableObject):
    oResponseObject = oTableObject.put_item(Item=oNewItemObject)
    return oResponseObject

def scanItem(oTableObject, oDynQueryObject):

    if oDynQueryObject == None:

        oDynResponseObject = oTableObject.scan()
        oItemList = oDynResponseObject['Items']

        while 'LastEvaluatedKey' in oDynResponseObject:
            oDynResponseObject = oTableObject.scan(ExclusiveStartKey=oDynResponseObject['LastEvaluatedKey'])
            oItemList.extend(oDynResponseObject['Items'])

        oDataResponse = {
            "d": {
                "__count": len(oItemList),
                "results": oItemList
            }
        }

        return oDataResponse
    
    else:

        oExpressionAttributeValues = {}
        oExpressionAttributeNames = {}
        sKeyConditionExpression = ""

        for oConditionObject in oDynQueryObject:

            if oConditionObject["condition_value"][0] == '\'':
                oConditionObject["condition_value"] = oConditionObject["condition_value"][1:-1]
            else:
                oConditionObject["condition_value"] = int(oConditionObject["condition_value"][1:-1])

            oExpressionAttributeValues[':' + oConditionObject["condition_field"]] = oConditionObject["condition_value"]
            oExpressionAttributeNames['#' + oConditionObject["condition_field"]] = oConditionObject['condition_field']

            if sKeyConditionExpression == '':
                sKeyConditionExpression = "#" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]
            else:
                sKeyConditionExpression = sKeyConditionExpression + ' ' + oConditionObject['next_condition_logic'] + " #" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]

        oDynResponseObject = oTableObject.scan(
            FilterExpression=sKeyConditionExpression,
            ExpressionAttributeValues=oExpressionAttributeValues,
            ExpressionAttributeNames=oExpressionAttributeNames
        )
        oItemList = oDynResponseObject['Items']

        while 'LastEvaluatedKey' in oDynResponseObject:
            oDynResponseObject = oTableObject.scan(
                FilterExpression=sKeyConditionExpression,
                ExpressionAttributeValues=oExpressionAttributeValues,
                ExpressionAttributeNames=oExpressionAttributeNames,
                ExclusiveStartKey=oDynResponseObject['LastEvaluatedKey']
            )
            oItemList.extend(oDynResponseObject['Items'])

        oDataResponse = {
            "d": {
                "__count": len(oItemList),
                "results": oItemList
            }
        }

        return oDataResponse
    
def parseDynQueryString(sUrlQueryString, oConditionlist):
    
    #field_1 eq 'field_1' and field_2 eq 'field_2/1.json'
    #(FirstName ne 'Mary' and LastName ne 'White') and UserName ne 'marywhite'

    # initialize variable
    sExpectedElement = 'condition_field'
    bStartValueQuote = False
    sJoinValue = ""
    sQueryList = sUrlQueryString.split(' ')
    oNewConditionList = oConditionlist
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

def queryItemFirst(oTableObject, oDynQueryObject):

    oExpressionAttributeValues = {}
    oExpressionAttributeNames = {}
    sKeyConditionExpression = ""

    for oConditionObject in oDynQueryObject:

        if oConditionObject["condition_value"][0] == '\'':
            oConditionObject["condition_value"] = oConditionObject["condition_value"][1:-1]
        else:
            oConditionObject["condition_value"] = int(oConditionObject["condition_value"][1:-1])

        oExpressionAttributeValues[':' + oConditionObject["condition_field"]] = oConditionObject["condition_value"]
        oExpressionAttributeNames['#' + oConditionObject["condition_field"]] = oConditionObject['condition_field']

        if sKeyConditionExpression == '':
            sKeyConditionExpression = "#" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]
        else:
            sKeyConditionExpression = sKeyConditionExpression + ' ' + oConditionObject['next_condition_logic'] + " #" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]

    oDynResponseObject = oTableObject.query(
        ExpressionAttributeValues = oExpressionAttributeValues,
        ExpressionAttributeNames = oExpressionAttributeNames,
        KeyConditionExpression=sKeyConditionExpression                
    )
    oItemList = oDynResponseObject['Items']

    oDataResponse = {
        "d": {
            "__count": len(oItemList),
            "results": oItemList
        }
    }

    return oDataResponse

def isKey(sFieldName, oKeyObject):
    
    if oKeyObject == None:
        return True
    
    if sFieldName in oKeyObject:
        return True
    else:
        return False
    
def queryItem(oTableObject, oDynQueryObject, oKeyObject):

    oExpressionAttributeValues = {}
    oExpressionAttributeNames = {}
    sKeyConditionExpression = ""
    sFilterConditionExpression = ""
    
    for oConditionObject in oDynQueryObject:

        if oConditionObject["condition_value"][0] == '\'':
            oConditionObject["condition_value"] = oConditionObject["condition_value"][1:-1]
        else:
            oConditionObject["condition_value"] = int(oConditionObject["condition_value"][1:-1])

        oExpressionAttributeValues[':' + oConditionObject["condition_field"]] = oConditionObject["condition_value"]
        oExpressionAttributeNames['#' + oConditionObject["condition_field"]] = oConditionObject['condition_field']
        
        if isKey(oConditionObject['condition_field'], oKeyObject):
            
            if sKeyConditionExpression == '':
                sKeyConditionExpression = "#" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]
            else:
                sKeyConditionExpression = sKeyConditionExpression + ' ' + oConditionObject['next_condition_logic'] + " #" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]
        
        else:
 
            if sFilterConditionExpression == '':
                sFilterConditionExpression = "#" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]
            else:
                sFilterConditionExpression = sFilterConditionExpression + ' ' + oConditionObject['next_condition_logic'] + " #" + oConditionObject["condition_field"] + ' ' + oConditionObject['condition_op'] + " :" + oConditionObject["condition_field"]
            
    if sKeyConditionExpression != "" and sFilterConditionExpression != "":
        
        oDynResponseObject = oTableObject.query(
            ExpressionAttributeValues = oExpressionAttributeValues,
            ExpressionAttributeNames = oExpressionAttributeNames,
            KeyConditionExpression=sKeyConditionExpression,
            FilterConditionExpress=sFilterConditionExpression
        )

        oItemList = oDynResponseObject['Items']
    
        while 'LastEvaluatedKey' in oDynResponseObject:
            oDynResponseObject = oTableObject.query(
                ExpressionAttributeValues = oExpressionAttributeValues,
                ExpressionAttributeNames = oExpressionAttributeNames,
                KeyConditionExpression=sKeyConditionExpression,
                FilterExpression=sFilterConditionExpression,
                ExclusiveStartKey=oDynResponseObject['LastEvaluatedKey']
            )
            oItemList.extend(oDynResponseObject['Items'])
    
    elif sKeyConditionExpression == "" and sFilterConditionExpression != "":

        oDynResponseObject = oTableObject.scan(
            ExpressionAttributeValues = oExpressionAttributeValues,
            ExpressionAttributeNames = oExpressionAttributeNames,
            FilterExpression=sFilterConditionExpression
        )

        oItemList = oDynResponseObject['Items']
    
        while 'LastEvaluatedKey' in oDynResponseObject:
            oDynResponseObject = oTableObject.scan(
                ExpressionAttributeValues = oExpressionAttributeValues,
                ExpressionAttributeNames = oExpressionAttributeNames,
                FilterExpression=sFilterConditionExpression,
                ExclusiveStartKey=oDynResponseObject['LastEvaluatedKey']
            )
            oItemList.extend(oDynResponseObject['Items'])
            
    else:
        
        oDynResponseObject = oTableObject.query(
            ExpressionAttributeValues = oExpressionAttributeValues,
            ExpressionAttributeNames = oExpressionAttributeNames,
            KeyConditionExpression=sKeyConditionExpression                
        )
        
        oItemList = oDynResponseObject['Items']
    
        while 'LastEvaluatedKey' in oDynResponseObject:
            oDynResponseObject = oTableObject.query(
                ExpressionAttributeValues = oExpressionAttributeValues,
                ExpressionAttributeNames = oExpressionAttributeNames,
                KeyConditionExpression=sKeyConditionExpression,
                ExclusiveStartKey=oDynResponseObject['LastEvaluatedKey']
            )
            oItemList.extend(oDynResponseObject['Items'])
            
    #oResponseObject = oTableObject.query(
    #    ExpressionAttributeValues = {":field_1": "dpl-field_1",":field_2": "field_2/2000000000000000000.json"},
    #    ExpressionAttributeNames = {"#field_1": "field_1","#field_2": "field_2"},
    #    KeyConditionExpression="#field_1 = :field_1 and #field_2 >= :field_2"                
    #)

    oDataResponse = {
        "d": {
            "__count": len(oItemList),
            "results": oItemList
        }
    }

    return oDataResponse

def deleteItem(oKeyObject, oTableObject):

    oResponseObject = oTableObject.delete_item(Key=oKeyObject)

    return oResponseObject

def getKeyObject(sTableName, oTableKeyObject):
    
    # example of oKeyObject
    #oTableKeyObject = {
    #    "table_name_1": ["field_1","field_2"],
    #    "table_name_2": ["field_1","field_2"]
    #}
    
    if sTableName in oTableKeyObject:
        return oTableKeyObject[sTableName]
    else:
        return None
        
class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)