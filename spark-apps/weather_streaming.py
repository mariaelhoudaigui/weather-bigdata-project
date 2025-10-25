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
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

# ============================================
# PARSING ET NETTOYAGE DES DONNÉES
# ============================================
weather_df = weather_stream \
    .select(from_json(col("value").cast("string"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("temperature", col("température").cast("double")) \
    .withColumn("humidity", col("humidité").cast("double")) \
    .withColumn("pressure", col("pression").cast("double")) \
    .withColumn("wind_speed_num", col("wind_speed").cast("double")) \
    .withColumn("feels_like_num", col("feels_like").cast("double")) \
    .withColumn("min_temp_num", col("min_temp").cast("double")) \
    .withColumn("max_temp_num", col("max_temp").cast("double")) \
    .withColumn("lat", col("latitude").cast("double")) \
    .withColumn("lon", col("longitude").cast("double")) \
    .withColumn("event_time", from_unixtime(col("timestamp").cast("long"))) \
    .withColumn("timestamp_dt", to_timestamp(col("event_time")))

# ============================================
# ENRICHISSEMENT DES DONNÉES
# ============================================

# 1. CALCUL DU POINT DE ROSÉE (Dew Point)
# Formule : Td ≈ T - ((100 - RH)/5)
enriched_weather = weather_df.withColumn(
    "dew_point",
    round(col("temperature") - ((100 - col("humidity")) / 5), 2)
)

# 2. INDICE DE CHALEUR (Heat Index)
# Formule simplifiée : HI = T + 0.5555 * (6.11 * e^(5417.7530 * (1/273.16 - 1/(Td+273.16))) - 10)
enriched_weather = enriched_weather.withColumn(
    "heat_index",
    when(col("temperature") >= 27,
         round(col("temperature") + 0.33 * col("humidity") - 0.70 * col("wind_speed_num") - 4.00, 2)
    ).otherwise(col("temperature"))
)

# 3. INDICE DE REFROIDISSEMENT ÉOLIEN (Wind Chill)
# Formule : WC = 13.12 + 0.6215*T - 11.37*V^0.16 + 0.3965*T*V^0.16
enriched_weather = enriched_weather.withColumn(
    "wind_chill",
    when((col("temperature") <= 10) & (col("wind_speed_num") > 4.8),
         round(13.12 + 0.6215 * col("temperature") - 
               11.37 * pow(col("wind_speed_num"), 0.16) + 
               0.3965 * col("temperature") * pow(col("wind_speed_num"), 0.16), 2)
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
# AGRÉGATIONS PAR FENÊTRES TEMPORELLES
# ============================================

# Fenêtre de 15 minutes avec watermark de 30 minutes
windowed_stats = enriched_weather \
    .withWatermark("timestamp_dt", "30 minutes") \
    .groupBy(
        window(col("timestamp_dt"), "15 minutes"),
        col("city_name")
    ) \
    .agg(
        # Température
        round(avg("temperature"), 2).alias("avg_temp"),
        round(max("temperature"), 2).alias("max_temp"),
        round(min("temperature"), 2).alias("min_temp"),
        round(stddev("temperature"), 2).alias("stddev_temp"),
        
        # Humidité
        round(avg("humidity"), 2).alias("avg_humidity"),
        round(max("humidity"), 2).alias("max_humidity"),
        round(min("humidity"), 2).alias("min_humidity"),
        
        # Pression
        round(avg("pressure"), 2).alias("avg_pressure"),
        round(max("pressure"), 2).alias("max_pressure"),
        round(min("pressure"), 2).alias("min_pressure"),
        
        # Vent
        round(avg("wind_speed_num"), 2).alias("avg_wind_speed"),
        round(max("wind_speed_num"), 2).alias("max_wind_speed"),
        
        # Indices calculés
        round(avg("heat_index"), 2).alias("avg_heat_index"),
        round(avg("dew_point"), 2).alias("avg_dew_point"),
    
        # Compteurs
        count("*").alias("num_records"),
        sum(when(col("alert_type") != "NORMAL", 1).otherwise(0)).alias("num_alerts")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

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
    .option("path", "/tmp/weather_enriched") \
    .option("checkpointLocation", "/tmp/checkpoint_enriched") \
    .start()

# ============================================
# SORTIE 3 : STATISTIQUES AGRÉGÉES (Parquet)
# ============================================
query_stats = windowed_stats \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/tmp/weather_stats") \
    .option("checkpointLocation", "/tmp/checkpoint_stats") \
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
    .option("path", "/tmp/weather_alerts") \
    .option("checkpointLocation", "/tmp/checkpoint_alerts") \
    .start()

# ============================================
# ATTENDRE LA FIN DES STREAMS
# ============================================
print("=" * 60)
print("✅ Spark Streaming Started Successfully!")
print("=" * 60)
print(f"📊 Processing weather data for Casablanca")
print(f"📁 Output locations:")
print(f"   - Enriched data: /tmp/weather_enriched")
print(f"   - Statistics: /tmp/weather_stats")
print(f"   - Alerts: /tmp/weather_alerts")
print("=" * 60)

spark.streams.awaitAnyTermination()