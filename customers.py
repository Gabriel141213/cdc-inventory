from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os
from delta.tables import DeltaTable
load_dotenv()
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
REPO_PATH = os.getenv("REPO_PATH")

# Create SparkSession with Delta Lake 2.0.0
spark = SparkSession.builder \
    .appName("KafkaToS3") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .config("fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Definição do esquema da tabela customers
after_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
])

before_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
])
# Schema completo para o campo "payload"
payload_schema = StructType([
    StructField("before", before_schema, True),
    StructField("after", after_schema, True),
    StructField("source", StructType(), True),
    StructField("op", StringType(), True),  # Operation type (insert, update, delete)
    StructField("ts_ms", LongType(), True),
])

# Schema completo do valor Kafka
kafka_value_schema = StructType([
    StructField("schema", StructType(), True),
    StructField("payload", payload_schema, True),
])

# Lógica de upsert (inserir ou atualizar)
def upsert_to_delta(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Verificar se a tabela Delta existe, se não, criar
        try:
            delta_table = DeltaTable.forPath(spark, f"{REPO_PATH}/customers")
        except:
            # Criar a tabela Delta se não existir
            empty_df = spark.createDataFrame([], schema=after_schema)
            empty_df.write.format("delta").save(f"{REPO_PATH}/customers")
            delta_table = DeltaTable.forPath(spark, f"{REPO_PATH}/customers")
                
        # Separar os diferentes tipos de operação
        insert_df = batch_df.filter((col("op") == "c") | (col("op") == "r") | (col("op") == "u")).drop("op")
        delete_df = batch_df.filter(col("op") == "d").drop("op")

        # Merge (upsert)
        if not insert_df.isEmpty():
            delta_table.alias("target").merge(
                insert_df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdate(
                condition="target.id = source.id",
                set={
                    "first_name": col("source.first_name"),
                    "last_name": col("source.last_name"),
                    "email": col("source.email")
                }
            ).whenNotMatchedInsert(
                values={
                    "id": col("source.id"),
                    "first_name": col("source.first_name"),
                    "last_name": col("source.last_name"),
                    "email": col("source.email")
                }
            ).execute()

        # Deletar registros
        if not delete_df.isEmpty():
            delta_table.alias("target").delete(
                condition=col("target.id").isin(delete_df.select("id").rdd.flatMap(lambda x: x).collect())
            )

# Ler do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "dbserver1.inventory.customers") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kafka_value_schema).alias("data")) \
    .withColumn(
        "id",
        when(col("data.payload.op") == "d", col("data.payload.before.id"))
        .when(col("data.payload.op").isin("c", "u", "r"), col("data.payload.after.id"))
    ).withColumn(
        "first_name",
        when(col("data.payload.op") == "d", col("data.payload.before.first_name"))
        .when(col("data.payload.op").isin("c", "u", "r"), col("data.payload.after.first_name"))
    ).withColumn(
        "last_name",
        when(col("data.payload.op") == "d", col("data.payload.before.last_name"))
        .when(col("data.payload.op").isin("c", "u", "r"), col("data.payload.after.last_name"))
    ).withColumn(
        "email",
        when(col("data.payload.op") == "d", col("data.payload.before.email"))
        .when(col("data.payload.op").isin("c", "u", "r"), col("data.payload.after.email"))
    ).withColumn(
        "op",
        col("data.payload.op")
    ).select("id", "first_name", "last_name", "email", "op")

# Write stream com a lógica de upsert
query = df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/tmp/spark-checkpoints-customers") \
    .start()

query.awaitTermination()
