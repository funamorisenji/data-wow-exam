from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Process Parquet Data") \
    .config("spark.jars", "/Users/serpsaipong/Documents/gitrepo/data-wow/jars/postgresql-42.5.4.jar") \
    .config("spark.default.parallelism", 200) \
    .config("spark.sql.shuffle.partitions", 200) \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Load data from parquet
data = spark.read.parquet("/Users/serpsaipong/Documents/gitrepo/data-wow/data_sample").repartition(200)  # Adjust based on your workload & cluster size

# Note: You'll need to provide the correct PostgreSQL connection details below
db_properties = {
    "user": "datawow",
    "password": "dataengineer",
    "driver": "org.postgresql.Driver"
}
url = "jdbc:postgresql://localhost:5432/warehouse"

# Extract and transform Department data
department = data.select("department_name").distinct()
department = department.withColumn("department_id", F.monotonically_increasing_id()+1).cache()
department = department.select("department_id", "department_name")
# department.show()
department.write.jdbc(url, "department", "append", db_properties)

# Extract and transform Product data
product = data.select("product_name").distinct()
product = product.withColumn("product_id", F.monotonically_increasing_id()+1).cache()
product = product.select("product_id", "product_name")
# product.show()
product.write.jdbc(url, "product", "append", db_properties)

# Extract and transform Sensor data
sensor = data.select("sensor_serial", "department_name").distinct() \
    .join(department, ["department_name"]) \
    .select("sensor_serial", "department_id")
sensor = sensor.withColumn("sensor_id", F.monotonically_increasing_id()+1).cache()
sensor = sensor.select("sensor_id", "sensor_serial", "department_id")
# sensor.show()
sensor.write.jdbc(url, "sensor", "append", db_properties)

# Extract and transform DataLog data
data_log = data.join(sensor, ["sensor_serial"]) \
    .join(product, ["product_name"]) \
    .select((F.monotonically_increasing_id()+1).alias("log_id"), "create_at", "product_id", "sensor_id", "product_expire")
data_log = data_log.select("log_id", "create_at", "product_id", "sensor_id", "product_expire")
# data_log.show()
data_log.write.jdbc(url, "dataLog", "append", db_properties)

spark.stop()
