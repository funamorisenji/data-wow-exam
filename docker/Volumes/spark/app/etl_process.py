from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from yaml import safe_load
from loguru import logger as log
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

# Load database configurations
with open('/usr/local/spark/config/database_config.yml', 'r') as f:
    database_config = safe_load(f)

# Initialize Spark session
spark = (SparkSession.builder
        .appName("Process Parquet Data")
        .config("spark.default.parallelism", 1000)
        .config("spark.sql.shuffle.partitions", 1000)
        .config("spark.sql.adaptive.enabled", "true")
        # .config("spark.dynamicAllocation.enabled", "true")
        # .config("spark.shuffle.service.enabled", "true")
        .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Load data from parquet without forced repartition
data = spark.read.parquet("/usr/local/spark/resources/data_sample")

# Database connection details
db_properties = {
    "user": database_config['user'],
    "password": database_config['password'],
    "driver": database_config['driver']
}
url = f"jdbc:postgresql://{database_config['ip']}:{database_config['port']}/{database_config['database']}"

# Department table
log.info("Processing Department data")
department = data.select("department_name").distinct()
# department.show()
# log.info("Total Records: " + str(department.count()))

# check for the existing record in the department table
existing_department = spark.read.jdbc(url, "(SELECT department_name FROM department) as t", properties=db_properties)
# existing_department.show(n=20)
# log.info(f"Total existing records: {existing_department.count()}")

# Left join new and old record
new_department= department.join(existing_department, on="department_name", how="left_anti")
# Filter out rows where department_name exists in the existing_department DataFrame
# log.info(f"Total new records: {new_department.count()}")

# Write a new record to the database
if len(new_department.head(1)) > 0:
    log.info("Write new record to department table")
    new_department.write.option("numPartitions",20).jdbc(url, "department", "append", db_properties)
    log.info(f"Finish write new record to department table\n")
else:
    log.info("No new records to write to department table\n")


# Product table
log.info("Processing Product data")
product = data.select("product_name").distinct()
# check for the existing record in the product table
existing_product = spark.read.jdbc(url, "(SELECT product_name FROM product) as t", properties=db_properties)
# Left join new and old record
new_product= product.join(existing_product, on="product_name", how="left_anti")
# Filter out rows where department_name exists in the existing_department DataFrame
# new_product = new_product.filter(F.col("product_name").isNull())
# log.info(f"Total new records: {new_product.count()}")
# Write a new record to the database
if len(new_product.head(1)) > 0:
    log.info("Write new record to product table")
    new_product.write.option("numPartitions",20).jdbc(url, "product", "append", db_properties)
    log.info(f"Finish write new record to product table\n")
else:
    log.info("No new records to write to product table\n")

# Sensor table
log.info("Processing Sensor data")
# You'll fetch the department_id for each sensor_serial in your data.
joined_data = data.join(
    spark.read.jdbc(url, "department", properties=db_properties),
    on="department_name",
    how="left"
)
# Now, select the necessary columns to insert into the sensor table
sensors_to_insert = joined_data.select("sensor_serial", "department_id").distinct()
# Check if the sensor_serial already exists in the sensor table
existing_sensor = spark.read.jdbc(url, "(SELECT sensor_serial, department_id FROM sensor) as t", properties=db_properties)
# log.info(f"Total existing records: {existing_sensor.count()}")
# Filter out the sensors that already exist in the sensor table
new_sensor = sensors_to_insert.join(existing_sensor, on=["sensor_serial", "department_id"], how="left_anti").orderBy("department_id")
# log.info(f"Total new records: {new_sensor.count()}")
# Write a new record to the database
if len(new_sensor.head(1)) > 0:
    log.info("Write new record to sensor table")
    # Only write the sensor_serial and department_id columns
    new_sensor.write.option("numPartitions",20).jdbc(url, "sensor", "append", db_properties)
    log.info(f"Finish write new record to sensor table\n")
else:
    log.info("No new records to write to sensor table\n")


# DataLog table
log.info("Processing DataLog data")
# check for the existing record in the department table
existing_datalog = spark.read.jdbc(url, "(SELECT product_id, sensor_id, product_expire FROM datalog) as t", properties=db_properties)

# Join the data DataFrame with the sensor table to get the sensor_id for each sensor_serial
datalog_with_sensor_id = data.join(
    spark.read.jdbc(url, "sensor", properties=db_properties),
    on="sensor_serial",
    how="left"
)
# Join the resulting DataFrame with the product table to get the product_id for each product_name
datalog_with_ids = datalog_with_sensor_id.join(
    spark.read.jdbc(url, "product", properties=db_properties),
    on="product_name",
    how="left"
)
# Select the necessary columns to insert into the datalog table
datalog_to_insert = datalog_with_ids.select(
    F.current_timestamp().alias("create_at"),  # This assumes that you want the current timestamp for create_at. Modify if you have a different column for this.
    "product_id",
    "sensor_id",
    "product_expire"
)
# check for new record
new_datalog = datalog_to_insert.join(
    existing_datalog,
    on=["product_id", "sensor_id", "product_expire"],
    how="left_anti"
).orderBy("create_at")

if len(new_datalog.head(1)) > 0:
    # Write the resulting DataFrame to the datalog table
    # log.info(f"Inserting {datalog_to_insert.count()} records into datalog table")
    log.info("Write new records to datalog table")
    datalog_to_insert.write.option("numPartitions", 20).jdbc(url, "datalog", "append", db_properties)
    log.info("Finished inserting records into datalog table")
else:
    log.info("No new records to write to datalog table\n")


log.info("ETL Successfully")
spark.stop()
