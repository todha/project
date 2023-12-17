from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import FloatType

# Tạo SparkSession
spark = SparkSession.builder.appName("CreditCardTotalAmount").getOrCreate()

# Đường dẫn tới file CSV hoặc DataFrame
file_path = "path/to/your/csvfile.csv"

# Đọc dữ liệu từ CSV vào DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Chuyển đổi cột "Amount" sang kiểu dữ liệu Float
df = df.withColumn("Amount", df["Amount"].cast(FloatType()))

# Loại bỏ dấu $ từ cột "Amount" và tính tổng số tiền
total_amount_df = df.withColumn("Amount", regexp_replace("Amount", "\\$", "").cast(FloatType())) \
                   .groupBy().sum("Amount").withColumnRenamed("sum(Amount)", "TotalAmount")

# Hiển thị kết quả
total_amount_df.show()
