import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id = '', aws_secret_access_key = '')

try:
	s3.create_bucket(Bucket = 'myawsbucket88472', CreateBucketConfiguration = {'LocationConstraint': 'us-east-2'})
except:
	print("bucket may already exist")

#s3.Object('myawsbucket88472', 'test.png').put(Body=open('C:\\Users\\jarod_000\\Desktop\\CS 1660\\AWS HW\\test.png', 'rb'))

dyndb = boto3.resource('dynamodb', region_name = 'us-east-2', aws_access_key_id = '', aws_secret_access_key = '')

table = ""

try:
	table = dyndb.create_table(
		TableName = 'DataTable',
		KeySchema = [
			{ 'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
			{ 'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
		],
		AttributeDefinitions = [
			{ 'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
			{ 'AttributeName': 'RowKey', 'AttributeType': 'S'}
		],
		ProvisionedThroughput={
			'ReadCapacityUnits': 5,
			'WriteCapacityUnits': 5
		}
	)
except:
	table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName = 'DataTable')

print(table.item_count)	

urlbase = "https://s3-us-east-2.amazonaws.com/myawsbucket88472/"
with open('C:\\Users\\jarod_000\\Desktop\\CS 1660\\AWS HW\\datafiles\\experiments.csv', 'r') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	next(csvf) # Skip header
	for item in csvf:
		print(item)
		body = open('C:\\Users\\jarod_000\\Desktop\\CS 1660\\AWS HW\\datafiles\\'+item[4], 'rb')
		s3.Object('myawsbucket88472', item[3]).put(Body=body )
		md = s3.Object('myawsbucket88472', item[3]).Acl().put(ACL='public-read')

		url = "https://s3-us-east-2.amazonaws.com/myawsbucket88472/"+item[4]
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
		'description' : item[4], 'date' : item[2], 'url':url}
		try:
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")

response = table.get_item(
	Key={
		'PartitionKey': 'experiment2',
		'RowKey': 'data2'
	}
)

item = response['Item']
print(item)
