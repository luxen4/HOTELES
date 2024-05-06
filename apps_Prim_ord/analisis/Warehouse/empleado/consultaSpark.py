from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("Leer y procesar con Spark") \
.config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
.config("spark.hadoop.fs.s3a.access.key", 'test') \
.config("spark.hadoop.fs.s3a.secret.key", 'test') \
.config("spark.sql.shuffle.partitions", "4") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.jars","./postgresql-42.7.3.jar") \
.config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
.master("local[*]") \
.getOrCreate()


jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

df = spark.read.jdbc(url=jdbc_url, table="warehouseempleado", properties=connection_properties)
df.createOrReplaceTempView("tabla_spark")

df_resultado = spark.sql(""" SELECT nombre, posicion, fecha_contratacion, nombre_hotel FROM tabla_spark """)

df_resultado.show()  # Mostrar el resultado de la consulta
spark.stop()




### Empleado
# Pregunta 3: ¿Quiénes son los empleados que trabajan en cada restaurante, junto
# con sus cargos y fechas de contratación?

# nombre_empleado  cargo_empleado  nombre_restaurante fecha_contratacion 
