# 📊 Risk Report Generator using PySpark

This project demonstrates how to build a **data transformation pipeline using PySpark**, designed to run on AWS Glue or locally for testing. The script reads raw user risk data, applies masking, formatting, and validation logic, and generates a clean risk report with logs.

---

## 🚀 Features

- ✅ Mask sensitive phone numbers (e.g., `9876543210` → `*******10`)
- ✅ Format phone numbers to `+91-XXX-XXX-XX-12`
- ✅ Validate Indian PIN codes (6-digit only)
- ✅ Clean up nulls and standardize fields
- ✅ Modular, log-enabled PySpark script

---

## 🧰 Tech Stack

- Python 3.8+
- PySpark 3.4.x
- AWS Glue (for production)
- Local development with logging

---

Note: This project runs locally using PySpark but is designed to be AWS Glue-compatible. To run it on AWS Glue, replace local paths with S3 paths and use GlueContext instead of SparkSession.

