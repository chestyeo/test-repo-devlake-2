import boto3
import json
from decimal import Decimal

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Specify your table name
table_name = 'this_is_not_rds'

# Get the table
table = dynamodb.Table(table_name)

# Scan the table to get all items
response = table.scan()

# Extract the items from the response
items = response['Items']

# Handle pagination if there are more than 1MB of data
while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    items.extend(response['Items'])

# Function to format each item
def format_item(item):
    return {
        "TagKey": {
            "S": item.get('TagKey', '')
        },
        "TagValue": {
            "S": item.get('TagValue', '')
        },
        "TagDescription": {"S": item.get('TagDescription', '')}
    }

# Format all items
formatted_items = [format_item(item) for item in items]

# Write the formatted items to a JSON file
output_file = f'{table_name}_data.json'
with open(output_file, 'w') as f:
    json.dump(formatted_items, f, indent=2)

print(f"Data has been downloaded and saved to {output_file}")