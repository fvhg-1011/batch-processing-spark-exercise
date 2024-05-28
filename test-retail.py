from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col, datediff, max,lit

# buat spark session 
# spark utilis nya running dari luar karena kalau diatru di config error muluuu
spark = SparkSession.builder.appName("RetailAnalysis").getOrCreate()

# Read table dataa
retail_data = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://0.0.0.0:5432/warehouse")
    .option("dbtable", "retail")
    .option("user", "user")
    .option("password", "password")
    .load()
)


# simple data analysis
# aggregasi simpleee untuk count jumlah negara
agg_df = retail_data.groupBy("country").count()

# show aggeragsi
print("Aggregation Results:")
agg_df.show()

# rewrite jadi csv
agg_df.write.mode("overwrite").csv("agg_output.csv")

# Churn Analysis

# mengelompokkan data based on customer id
churn_df = retail_data.groupBy("customerid").agg(
    max("invoicedate").alias("last_order_date"),  #tanggal pesanan terakhir untuk setiap customer
    count("*").alias("order_count") # total jumlah pesanan tiap pelanggan
)

churn_threshold_date = lit("2010-09-01")  # asumsi batas thresholdnya

# buat kondisi churn apabila lebih dari 90 hari maka jadi churn(1) dan jika tidak maka tidak churn (0)
churn_df = churn_df.withColumn(
    "is_churned",
    when(datediff(churn_threshold_date, col("last_order_date")) > 90, 1).otherwise(0),
)

# show churn
print("Hasil analysis churn:")
churn_df.show()

# masukin jadi csv
churn_df.write.mode("overwrite").csv("churn_output.csv")

# matiin spark session(biar prosesnya g nyangkut)
spark.stop()

