import os
import sys

from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

def process_line(line):
    client, _, amount = line.split(",")
    return client, float(amount)

for client, amount in sorted(sc.textFile("customer-orders.csv")
                                     .map(process_line)
                                     .reduceByKey(lambda v1, v2: v1 + v2)
                                     .collectAsMap().items(), key=lambda pair: pair[1]):
    print(f"Client {client} amount {round(amount, 2)}")