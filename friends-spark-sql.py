import os
import sys

from pyspark.sql import SparkSession, Row

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("friends-spark-sql").getOrCreate()

def mapper(line):
    id, name, age, friends = line.split(",")
    return Row(
        id=int(id),
        name=str(name),
        age=int(age),
        friends=int(friends)
    )

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

df = spark.createDataFrame(people)
df.printSchema()
df.createOrReplaceTempView("people")

df.filter(df.age < 21).show(5)

spark.sql("""
    select age, round(avg(friends), 2) as avg_count 
      from people 
     group by age 
     order by 2 desc
""").show(5)

spark.stop()


