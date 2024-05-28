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


# data analysis
# aggregasi simpleee
agg_df = retail_data.groupBy("country").count()

# show aggeragsi
print("Aggregation Results:")
agg_df.show()

# rewrite jadi csv
agg_df.write.mode("overwrite").csv("agg_output.csv")

# Churn Analysis
# assume invoice date jadi "date" dan  'customerid' jadi unique customers
churn_df = retail_data.groupBy("customerid").agg(
    max("invoicedate").alias("last_order_date"), count("*").alias("order_count")
)

# bikin batas threshold buat 90 hariii misalkan 
churn_threshold_date = lit("2010-09-01")  # asmussi batas thersholdnya
churn_df = churn_df.withColumn(
    "is_churned",
    when(datediff(churn_threshold_date, col("last_order_date")) > 90, 1).otherwise(0),
)

# show churn
print("Churn Analysis Results:")
churn_df.show()

# masukin jadi csv
churn_df.write.mode("overwrite").csv("churn_output.csv")

# matiin spark session(biar prosesnya g kebawa ke yg lain)
spark.stop()

