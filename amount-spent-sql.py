import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("amount-spent-sql").getOrCreate()

# schema = StructType([StructField("client_id", IntegerType(), False),
#                      StructField("item_id", IntegerType(), False),
#                      StructField("amount", DoubleType(), False)
# ])

schema = "client_id INT, item_id INT, amount DOUBLE"

orders = spark.read.schema(schema=schema).csv("customer-orders.csv")
orders.createOrReplaceTempView("orders")
orders.printSchema()

spark.sql("""
    select client_id, 
           round(sum(amount), 2) as total
      from orders
     group by client_id
     order by total desc 
""").show(5)

spark.stop()