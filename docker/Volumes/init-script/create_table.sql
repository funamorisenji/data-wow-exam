-- Creating sequences
CREATE SEQUENCE department_department_id_seq;
CREATE SEQUENCE sensor_sensor_id_seq;
CREATE SEQUENCE product_product_id_seq;
CREATE SEQUENCE datalog_log_id_seq;

-- Table: department
CREATE TABLE IF NOT EXISTS "department" (
  "department_id" BIGINT DEFAULT nextval('department_department_id_seq') PRIMARY KEY,
  "department_name" VARCHAR(255) UNIQUE
  -- "load_date" DATE
);

-- Table: sensor
CREATE TABLE IF NOT EXISTS "sensor" (
  "sensor_id" BIGINT DEFAULT nextval('sensor_sensor_id_seq') PRIMARY KEY,
  "sensor_serial" VARCHAR(255) UNIQUE,
  "department_id" BIGINT
  -- "load_date" DATE
);

-- Table: product
CREATE TABLE IF NOT EXISTS "product" (
  "product_id" BIGINT DEFAULT nextval('product_product_id_seq') PRIMARY KEY,
  "product_name" VARCHAR(255) UNIQUE
  -- "load_date" DATE
);

-- Table: datalog
CREATE TABLE IF NOT EXISTS "datalog" (
  "log_id" BIGINT DEFAULT nextval('datalog_log_id_seq') PRIMARY KEY,
  "create_at" TIMESTAMP,
  "product_id" BIGINT,
  "sensor_id" BIGINT,
  "product_expire" TIMESTAMP
  -- "load_date" DATE
);

-- Setting ownership of sequences
ALTER SEQUENCE department_department_id_seq OWNED BY "department"."department_id";
ALTER SEQUENCE sensor_sensor_id_seq OWNED BY "sensor"."sensor_id";
ALTER SEQUENCE product_product_id_seq OWNED BY "product"."product_id";
ALTER SEQUENCE datalog_log_id_seq OWNED BY "datalog"."log_id";

-- Adding Foreign Key Constraints
ALTER TABLE "sensor" ADD FOREIGN KEY ("department_id") REFERENCES "department" ("department_id");
ALTER TABLE "datalog" ADD FOREIGN KEY ("product_id") REFERENCES "product" ("product_id");
ALTER TABLE "datalog" ADD FOREIGN KEY ("sensor_id") REFERENCES "sensor" ("sensor_id");
