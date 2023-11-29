from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
from pyspark.sql.functions import from_json, col

iotSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("lclid", StringType(), False),
    StructField("day", StringType(), False),
    StructField("energy_max", FloatType(), False),
    StructField("energy_min", FloatType(), False),
    StructField("energy_sum", FloatType(), False),
    StructField("energy_median", FloatType(), False),
    StructField("energy_std", FloatType(), False),
])


spark = SparkSession \
    .builder \
    .appName("SparkStructuredStreaming") \
    .config("spark.cassandra.connection.host","172.18.0.5")\
    .config("spark.cassandra.connection.port","9042")\
    .config("spark.cassandra.auth.username","cassandra")\
    .config("spark.cassandra.auth.password","cassandra")\
    .config("spark.driver.host", "localhost")\
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "iot_03") \
    .option("delimeter", ",") \
    .option("startingOffsets", "earliest") \
    .load()

df.printSchema()

df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), iotSchema).alias("data")).select("data.*")
df1.printSchema()

def writeToCassandra(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="energy", keyspace="iot") \
        .save()


df1.writeStream \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .start()\
    .awaitTermination()

df1.show()
