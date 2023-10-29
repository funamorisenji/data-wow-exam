from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Process Parquet Data") \
    .config("spark.jars", "/Users/serpsaipong/Documents/gitrepo/data-wow/jars/postgresql-42.5.4.jar") \
    .config("spark.default.parallelism", 200) \
    .config("spark.sql.shuffle.partitions", 200) \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Load data from parquet
data = spark.read.parquet("/Users/serpsaipong/Documents/gitrepo/data-wow/data_sample")

# Database properties
db_properties = {
    "user": "datawow",
    "password": "dataengineer",
    "driver": "org.postgresql.Driver"
}
url = "jdbc:postgresql://localhost:5432/warehouse"

# Extract and transform Department data
department = data.select("department_name").distinct()
department = department.withColumn("department_id", F.monotonically_increasing_id()+1).cache()
department.write.jdbc(url, "department", "append", db_properties)

# Extract and transform Product data
product = data.select("product_name").distinct().withColumn("product_id", F.monotonically_increasing_id()+1).cache()
product.write.jdbc(url, "product", "append", db_properties)

# Extract and transform Sensor data
sensor = data.select("sensor_serial", "department_name").distinct() \
    .join(broadcast(department), ["department_name"]) \
    .withColumn("sensor_id", F.monotonically_increasing_id()+1).cache()
sensor.write.jdbc(url, "sensor", "append", db_properties)

# Extract and transform DataLog data
data_log = data.join(broadcast(sensor), ["sensor_serial"]) \
    .join(broadcast(product), ["product_name"]) \
    .select((F.monotonically_increasing_id()+1).alias("log_id"), "create_at", "product_id", "sensor_id", "product_expire")
data_log.write.jdbc(url, "dataLog", "append", db_properties)

spark.stop()
