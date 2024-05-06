import boto3
from botocore.exceptions import ClientError
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')
        
def upload_file_to_s3(file_path, bucket_name, s3_key):
    try:
        with open(file_path, 'rb') as file:
            file_content = file.read()

        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)

        print(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except ClientError as e:
        print(f"An error occurred: {e}")


bucket_name = 'my-local-bucket'                     # Create a bucket
s3.create_bucket(Bucket=bucket_name)

# Example usage:
file_path = "./../../1_data_bda/json/data_pokemon.json"  # Replace with the local file path
s3_key = "data_pokemon.json"
upload_file_to_s3(file_path, bucket_name, s3_key)

file_path = "./../../1_data_bda/text/data_battle_records.txt"  # Replace with the local file path
s3_key = "data_battle_records.txt"
upload_file_to_s3(file_path, bucket_name, s3_key)


file_path = "./../../1_data_bda/mongodb/data_events.json"  # Replace with the local file path
s3_key = "data_events.json"
upload_file_to_s3(file_path, bucket_name, s3_key)



### Ver el contenido de un archivo virtual ###
    # awslocal s3 cp s3://my-local-bucket/dataPokemon.json ./pokemon_events.json 
    # cat pokemon_events.json   # En sistemas Unix-like
    # type dataPokemon.json  # En Windows








'''
import boto3
import json
import io

# Initialize Boto3 client for S3

s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')
        
bucket_name = 'my-local-bucket'
file_key = 'dataPokemon.json'

response = s3.get_object(Bucket=bucket_name, Key=file_key)
json_content = response['Body'].read().decode('utf-8')

data = json.loads(json_content)

# Display the JSON data
#print(data)
'''