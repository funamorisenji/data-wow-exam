CREATE TABLE IF NOT EXISTS "department" (
  "department_id" BIGSERIAL PRIMARY KEY,
  "department_name" VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS "sensor" (
  "sensor_id" BIGINT PRIMARY KEY,
  "sensor_serial" VARCHAR(255) UNIQUE,
  "department_id" BIGINT
);


CREATE TABLE IF NOT EXISTS "product" (
  "product_id" BIGSERIAL PRIMARY KEY,
  "product_name" VARCHAR(255) UNIQUE
);


CREATE TABLE IF NOT EXISTS "datalog" (
  "log_id" BIGSERIAL PRIMARY KEY,
  "create_at" TIMESTAMP,
  "product_id" BIGINT,
  "sensor_id" BIGINT,
  "product_expire" TIMESTAMP
);


-- Adding Foreign Key Constraints
ALTER TABLE "sensor" ADD FOREIGN KEY ("department_id") REFERENCES "department" ("department_id");

ALTER TABLE "datalog" ADD FOREIGN KEY ("product_id") REFERENCES "product" ("product_id");

ALTER TABLE "datalog" ADD FOREIGN KEY ("sensor_id") REFERENCES "sensor" ("sensor_id");
