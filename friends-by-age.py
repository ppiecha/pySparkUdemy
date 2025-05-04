import os
import sys
from pathlib import Path

from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

def process_line(line):
    _, _, age, friends, *_ = line.split(",")
    return int(age), int(friends)

print("path exists", Path("fakefriends.csv").exists())

lines = sc.textFile("fakefriends.csv")

avg_per_age = (lines.map(process_line)
 .mapValues(lambda friends: (friends, 1))
 .reduceByKey(lambda pair1, pair2: (pair1[0] + pair2[0], pair1[1] + pair2[1]))
 .mapValues(lambda pair: pair[0] / pair[1])
 .collect())

for item in sorted(avg_per_age):
    print(f"Age {item[0]} friends {round(item[1])}")