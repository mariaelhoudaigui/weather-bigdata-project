from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math

# ============================================
# CONFIGURATION SPARK
# ============================================
spark = SparkSession.builder \
    .appName("CasablancaWeatherStreamProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================
# SCHEMA DES DONNÉES
# ============================================
weather_schema = StructType([
    StructField("date", StringType()),
    StructField("weather_description", StringType()),
    StructField("latitude", StringType()),
    StructField("pression", StringType()),
    StructField("humidité", StringType()),
    StructField("feels_like", StringType()),
    StructField("city_name", StringType()),
    StructField("local_time", StringType()),
    StructField("min_temp", StringType()),
    StructField("wind_speed", StringType()),
    StructField("température", StringType()),
    StructField("max_temp", StringType()),
    StructField("timestamp", StringType()),
    StructField("longitude", StringType())
])

# ============================================
# LECTURE DEPUIS KAFKA
# ============================================
weather_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","kafka:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

# ============================================
# PARSING ET NETTOYAGE DES DONNÉES
# ============================================
weather_df = weather_stream \
    .select(from_json(col("value").cast("string"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("temperature", round(col("température").cast("double")).cast("integer")) \
    .withColumn("humidity", col("humidité").cast("double").cast("integer")) \
    .withColumn("pressure", col("pression").cast("double").cast("integer")) \
    .withColumn("wind_speed_num", col("wind_speed").cast("double")) \
    .withColumn("feels_like_num", round(col("feels_like").cast("double")).cast("integer")) \
    .withColumn("min_temp_num", round(col("min_temp").cast("double")).cast("integer")) \
    .withColumn("max_temp_num", round(col("max_temp").cast("double")).cast("integer")) \
    .withColumn("lat", col("latitude").cast("double")) \
    .withColumn("lon", col("longitude").cast("double")) \
    .withColumn("event_time", from_unixtime(col("timestamp").cast("long"))) \
    .withColumn("timestamp_dt", to_timestamp(col("event_time"))) \
    .drop("température", "humidité", "pression", "feels_like", "min_temp", "max_temp", 
          "latitude", "longitude", "wind_speed") 

# ============================================
# ENRICHISSEMENT DES DONNÉES
# ============================================

# 1. CALCUL DU POINT DE ROSÉE (Dew Point) - arrondi sans virgule
enriched_weather = weather_df.withColumn(
    "dew_point",
    round(col("temperature") - ((100 - col("humidity")) / 5)).cast("integer")
)

# 2. INDICE DE CHALEUR (Heat Index) - arrondi sans virgule
enriched_weather = enriched_weather.withColumn(
    "heat_index",
    when(col("temperature") >= 27,
         round(col("temperature") + 0.33 * col("humidity") - 0.70 * col("wind_speed_num") - 4.00).cast("integer")
    ).otherwise(col("temperature"))
)

# 3. INDICE DE REFROIDISSEMENT ÉOLIEN (Wind Chill) - arrondi sans virgule
enriched_weather = enriched_weather.withColumn(
    "wind_chill",
    when((col("temperature") <= 10) & (col("wind_speed_num") > 4.8),
         round(13.12 + 0.6215 * col("temperature") - 
               11.37 * pow(col("wind_speed_num"), 0.16) + 
               0.3965 * col("temperature") * pow(col("wind_speed_num"), 0.16)).cast("integer")
    ).otherwise(col("temperature"))
)

# 4. CLASSIFICATION DES CONDITIONS MÉTÉO
enriched_weather = enriched_weather.withColumn(
    "weather_category",
    when(col("weather_description").like("%clear%"), "Clear")
    .when(col("weather_description").like("%cloud%"), "Cloudy")
    .when(col("weather_description").like("%rain%"), "Rainy")
    .when(col("weather_description").like("%storm%"), "Stormy")
    .when(col("weather_description").like("%snow%"), "Snowy")
    .when(col("weather_description").like("%fog%"), "Foggy")
    .otherwise("Other")
)

# 5. NIVEAU DE CONFORT
enriched_weather = enriched_weather.withColumn(
    "comfort_level",
    when(col("temperature").between(18, 24) & col("humidity").between(30, 60), "Comfortable")
    .when(col("temperature") > 30, "Very Hot")
    .when(col("temperature") < 10, "Cold")
    .when(col("humidity") > 80, "Humid")
    .otherwise("Moderate")
)

# 6. DÉTECTION D'ANOMALIES
enriched_weather = enriched_weather.withColumn(
    "is_extreme_temp",
    when((col("temperature") > 40) | (col("temperature") < 0), True).otherwise(False)
).withColumn(
    "is_high_wind",
    when(col("wind_speed_num") > 50, True).otherwise(False)
).withColumn(
    "is_pressure_anomaly",
    when((col("pressure") < 980) | (col("pressure") > 1040), True).otherwise(False)
).withColumn(
    "alert_type",
    when(col("is_extreme_temp"), "EXTREME_TEMPERATURE")
    .when(col("is_high_wind"), "HIGH_WIND")
    .when(col("is_pressure_anomaly"), "PRESSURE_ANOMALY")
    .otherwise("NORMAL")
)


# ============================================
# SORTIE 1 : CONSOLE (pour debug)
# ============================================
query_console = enriched_weather \
    .select(
        "timestamp_dt",
        "city_name",
        "temperature",
        "humidity",
        "pressure",
        "heat_index",
        "dew_point",
        "wind_chill",
        "comfort_level",
        "alert_type"
    ) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# ============================================
# SORTIE 2 : FICHIERS JSON (données enrichies)
# ============================================
query_json = enriched_weather \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/opt/spark-output/weather_enriched") \
    .option("checkpointLocation", "/opt/spark-output/checkpoint_enriched") \
    .start()



# ============================================
# SORTIE 4 : ALERTES (filtrées)
# ============================================
alerts_stream = enriched_weather \
    .filter(col("alert_type") != "NORMAL") \
    .select(
        "timestamp_dt",
        "city_name",
        "alert_type",
        "temperature",
        "wind_speed_num",
        "pressure"
    )

query_alerts = alerts_stream \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/opt/spark-output/weather_alerts") \
    .option("checkpointLocation", "/opt/spark-output/checkpoint_alerts") \
    .start()

# ============================================
# ATTENDRE LA FIN DES STREAMS
# ============================================
print("=" * 60)
print("Spark Streaming Started Successfully!")
print("=" * 60)
print(f"Processing weather data for Casablanca")
print(f"Output locations:")
print(f"   - Enriched data: /tmp/weather_enriched")
print(f"   - Statistics: /tmp/weather_stats")
print(f"   - Alerts: /tmp/weather_alerts")
print("=" * 60)

spark.streams.awaitAnyTermination()