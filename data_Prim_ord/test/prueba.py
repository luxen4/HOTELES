import boto3

# Crear el cliente de S3
# s3 = boto3.client('s3', aws_access_key_id='your_access_key_id', aws_secret_access_key='your_secret_access_key')
s3 = boto3.client('s3',endpoint_url='http://localhost:4566',aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')


# Intentar listar los buckets
try:
    response = s3.list_buckets()
    print("Conexión exitosa. Buckets disponibles:")
    for bucket in response['Buckets']:
        print(f"- {bucket['Name']}")
except Exception as e:
    print("Error al conectar con S3 de LocalStack:", e)



# Crear el objeto en el bucket para simular la carpeta
s3.put_object(Bucket='my-local-bucket', Key=('prueba/'))

# Intentar listar los objetos en la carpeta
try:
    response = s3.list_objects_v2(Bucket='my-local-bucket', Prefix='prueba/')
    if 'Contents' in response:
        print("La carpeta contiene los siguientes objetos:")
        for obj in response['Contents']:
            print(f"- {obj['Key']}")
    else:
        print("La carpeta está vacía.")
except Exception as e:
    print("Error al listar los objetos en la carpeta:", e)
    
    
# Meter en la carpeta prueba


# Nombre del bucket y la carpeta
bucket_name = 'my-local-bucket'
folder_name = 'prueba/'

# Ruta local del archivo que quieres subir
local_file_path = 'sensoras.csv'

# Ruta en S3 donde quieres guardar el archivo
s3_key = folder_name + local_file_path

# Subir el archivo al bucket de S3
with open(local_file_path, 'rb') as file:
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file)

print(f"Archivo '{local_file_path}' subido correctamente al bucket '{bucket_name}' en la carpeta '{folder_name}'.")