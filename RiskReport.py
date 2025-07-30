import findspark
findspark.init()
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# ---------------- Logger Setup ----------------
logging.basicConfig(
    filename="risk_report.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger()
logger.info("Spark job started")

# ---------------- Spark Setup ----------------
spark = SparkSession.builder.appName("RiskReportMiniPRJ").getOrCreate()
logger.info("SparkSession initialized")

# ---------------- Read Input Data ----------------
customers_csv_path = r"D:\\Development\\Python_Practice\\dataSet\\customers.csv"
transactions_csv_path = r"D:\\Development\\Python_Practice\\dataSet\\transactions.csv"

customers_df = spark.read.csv(customers_csv_path, header=True, inferSchema=True)
transactions_df = spark.read.csv(transactions_csv_path, header=True, inferSchema=True)
logger.info("Data loaded from CSV files")

# ---------------- Data Cleaning ----------------
transactions_clean = transactions_df.filter(
    (f.col("txn_id").isNotNull()) & (f.trim(f.col("txn_id")) != "") &
    (f.col("customer_id").isNotNull()) & (f.trim(f.col("customer_id")) != "")
)
customers_clean = customers_df.filter(
    (f.col("customer_id").isNotNull()) & (f.trim(f.col("customer_id")) != "")
)
logger.info("Null or empty txn_id and customer_id removed")

# ---------------- UDF Definitions ----------------
def mask_phone(phone_number):
    if phone_number is None:
        return phone_number
    phone_str = str(phone_number)
    try:
        if phone_str.isdigit() and len(phone_str) == 10:
            return (8 * 'X' + phone_str[-2:])
        else:
            return phone_number
    except:
        return phone_number

masked_phone_udf = f.udf(mask_phone, StringType())

email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z0-9]{2,}$'
customers_clean_valid_email = customers_clean.withColumn("isValidEmail", f.col("email").rlike(email_pattern))


def pincode(zipcode):
    if zipcode is None:
        return "Invalid zipcode"
    zip_str = str(zipcode)
    try:
        if zip_str.isdigit() and len(zip_str) == 6:
            return "Valid zipcode"
        else:
            return "Invalid zipcode"
    except:
        return "Invalid zipcode"

Valid_zip_udf = f.udf(pincode, StringType())


def masked_email(email):
    if email is None:
        return None
    email_str = str(email)
    try:
        if "@" in email_str:
            local, domain = email_str.split("@", 1)
            return (len(local) * "*" + "@" + domain)
        else:
            return "Invalid email"
    except:
        return "Invalid email"

mask_email_udf = f.udf(masked_email, StringType())

# ---------------- Feature Engineering ----------------
customers_clean_masked_df = customers_clean_valid_email \
    .withColumn("masked_phone_number", masked_phone_udf(f.col("phone"))) \
    .withColumn("is_valid_pincode", Valid_zip_udf(f.col("pin_code"))) \
    .withColumn("is_valid_email", mask_email_udf(f.col("email")))
logger.info("Applied UDFs for masking and validation")

transactions_with_txn_day = transactions_clean.withColumn("txn_day", f.date_format(f.to_date(f.col("txn_date")), "EEEE"))
transactions_with_amount = transactions_with_txn_day.withColumn("is_high_value", f.when(f.col("amount") > 1000, "Yes").otherwise("No"))

# ---------------- Join & Aggregation ----------------
joined_df = customers_clean_masked_df.join(transactions_with_amount, on="Customer_id", how="left")

window_spe = Window.partitionBy(f.col("txn_id"), f.col("txn_date")).orderBy(f.col("amount"), f.col("customer_id"))
customer_with_row_num = joined_df.withColumn("row_num", f.row_number().over(window_spe)).filter(f.col("row_num") == 1).drop("row_num")

customer_financial_summary = customer_with_row_num.groupBy("customer_id", "name", "email") \
    .agg(
        f.sum(f.when(f.col("txn_type") == "CREDIT", f.col("amount")).otherwise(0)).alias("total_credit"),
        f.sum(f.when(f.col("txn_type") == "DEBIT", f.col("amount")).otherwise(0)).alias("total_debit"),
        f.avg(f.col("amount")).alias("avg_txn_amount"),
        f.max(f.col("txn_date")).alias("last_txn_date"),
        f.count(f.col("txn_id")).alias("txn_count")
    )
logger.info("Generated customer financial summary")

# ---------------- Save Outputs ----------------
output_dir = r'D:\\Development\\Python_Practice\\dataSet'
os.makedirs(output_dir, exist_ok=True)

customer_summary_path = os.path.join(output_dir, "customer_financial_summary.parquet")
customer_financial_summary.write.mode("overwrite").parquet(customer_summary_path)
logger.info(f"Customer summary saved to: {customer_summary_path}")

# ---------------- Risky Customers Report ----------------
customers_clean_riskey_df = customers_clean_masked_df \
    .filter((f.col("is_valid_pincode") != "Valid zipcode") | (f.col("isValidEmail") != False)) \
    .select("customer_id", "name", "email", "pin_code", f.lit("Invalid Contact Info").alias("risk_reason"))

risky_by_daily_volume_df = customer_with_row_num.groupBy("customer_id", f.to_date("txn_date").alias("txn_day")) \
    .agg(f.count("txn_id").alias("daily_txn_count")) \
    .filter(f.col("daily_txn_count") > 3)

risky_by_volume_final_df = risky_by_daily_volume_df.join(customers_clean_masked_df, on="customer_id", how="inner") \
    .select("customer_id", "name", "email", "pin_code", f.lit("High Daily Transaction Volume").alias("risk_reason"))

risky_customers_final = customers_clean_riskey_df.unionByName(risky_by_volume_final_df).distinct()

risky_customers_path = os.path.join(output_dir, "risky_customers.csv")
risky_customers_final.write.mode("overwrite").csv(risky_customers_path, header=True)
logger.info(f"Risky customer report saved to: {risky_customers_path}")

logger.info("Spark job completed")
spark.stop()
