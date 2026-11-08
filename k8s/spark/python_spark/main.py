import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    current_timestamp,
    lit,
    lower,
    row_number,
    sum,
)
from pyspark.sql.window import Window

from logger import get_logger


logger = get_logger("daily-medallion")

TABLES = {
    "clients": "client_id",
    "cards": "card_number",
    "operation": "operation_id",
    "transactions": "transaction_id",
}
TECHNICAL_COLUMNS = {"__op", "__source_ts_ms", "__source_lsn", "__deleted"}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Не задана переменная {name}")
    return value


def latest_rows(bronze: DataFrame, primary_key: str) -> DataFrame:
    order = []
    if "__source_ts_ms" in bronze.columns:
        order.append(col("__source_ts_ms").desc_nulls_last())
    if "__source_lsn" in bronze.columns:
        order.append(col("__source_lsn").desc_nulls_last())

    window = Window.partitionBy(primary_key).orderBy(*order)
    return bronze.withColumn("_row_number", row_number().over(window)) \
        .filter(col("_row_number") == 1) \
        .drop("_row_number")


def active_rows(bronze: DataFrame, primary_key: str) -> DataFrame:
    latest = latest_rows(bronze, primary_key)
    if "__deleted" not in latest.columns:
        return latest
    deleted = coalesce(lower(col("__deleted").cast("string")), lit("false"))
    return latest.filter(deleted != "true")


def fit_to_bronze(data: DataFrame, bronze: DataFrame, operation: str) -> DataFrame:
    data = data \
        .withColumn("__op", lit(operation)) \
        .withColumn("__deleted", lit("true" if operation == "d" else "false")) \
        .withColumn("__source_ts_ms", current_timestamp()) \
        .withColumn("__source_lsn", lit(None).cast("long"))

    columns = []
    for field in bronze.schema:
        if field.name in data.columns:
            columns.append(col(field.name).cast(field.dataType).alias(field.name))
        else:
            columns.append(lit(None).cast(field.dataType).alias(field.name))
    return data.select(*columns)


def reconcile_table(
        spark: SparkSession,
        table: str,
        primary_key: str,
        postgres: dict,
        mysql: dict,
) -> DataFrame:
    source = spark.read.jdbc(postgres["url"], table, properties=postgres["properties"])
    bronze = spark.read.jdbc(mysql["url"], table, properties=mysql["properties"])
    current = active_rows(bronze, primary_key)

    missing = source.join(current.select(primary_key), primary_key, "left_anti")
    extra = current.join(source.select(primary_key), primary_key, "left_anti")

    missing_count = missing.count()
    extra_count = extra.count()
    logger.info("%s: отсутствует в bronze=%s, лишних в bronze=%s", table, missing_count, extra_count)

    if missing_count:
        fit_to_bronze(missing, bronze, "r").write.jdbc(
            mysql["url"], table, mode="append", properties=mysql["properties"]
        )
    if extra_count:
        fit_to_bronze(extra, bronze, "d").write.jdbc(
            mysql["url"], table, mode="append", properties=mysql["properties"]
        )

    repaired_bronze = spark.read.jdbc(mysql["url"], table, properties=mysql["properties"])
    silver = active_rows(repaired_bronze, primary_key)
    business_columns = [name for name in source.columns if name not in TECHNICAL_COLUMNS]
    return silver.select(*business_columns)


def write_iceberg(data: DataFrame, namespace: str, table: str) -> None:
    data.writeTo(f"iceberg.{namespace}.{table}").createOrReplace()


def main() -> None:
    spark = SparkSession.builder \
        .appName("daily-medallion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", required_env("S3_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", required_env("S3_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", required_env("S3_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    postgres = {
        "url": required_env("POSTGRES_URL"),
        "properties": {
            "user": required_env("POSTGRES_USER"),
            "password": required_env("POSTGRES_PASSWORD"),
            "driver": "org.postgresql.Driver",
        },
    }
    mysql = {
        "url": required_env("MYSQL_URL"),
        "properties": {
            "user": required_env("MYSQL_USER"),
            "password": required_env("MYSQL_PASSWORD"),
            "driver": "com.mysql.cj.jdbc.Driver",
        },
    }

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

        silver = {}
        for table, primary_key in TABLES.items():
            silver[table] = reconcile_table(spark, table, primary_key, postgres, mysql)
            write_iceberg(silver[table], "silver", table)

        client_turnover = silver["transactions"] \
            .join(silver["cards"], "card_number") \
            .join(silver["clients"], "client_id") \
            .groupBy("client_id", "full_name") \
            .agg(
            count("transaction_id").alias("transaction_count"),
            sum("amount").alias("turnover"),
            avg("amount").alias("average_amount"),
        )

        operation_summary = silver["transactions"] \
            .join(silver["operation"], "operation_id") \
            .groupBy("operation_id", "operation_name") \
            .agg(
            count("transaction_id").alias("transaction_count"),
            sum("amount").alias("total_amount"),
        )

        write_iceberg(client_turnover, "gold", "client_turnover")
        write_iceberg(operation_summary, "gold", "operation_summary")

        compact_tables = [
            *(f"silver.{table}" for table in TABLES),
            "gold.client_turnover",
            "gold.operation_summary",
        ]
        for table in compact_tables:
            spark.sql(f"""
                CALL iceberg.system.rewrite_data_files(
                    table => '{table}',
                    options => map('min-input-files', '2', 'target-file-size-bytes', '134217728')
                )
            """)


        logger.info("Ежедневный pipeline завершён")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()