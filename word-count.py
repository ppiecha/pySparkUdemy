import os
import re
import sys

from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

for word, count in sorted (sc.textFile("book.txt")
                                   .flatMap(lambda line: re.split("\\W+", line.lower()))
                                   .filter(lambda s: len(s) > 0)
                                   .countByValue()
                                   .items(), key=lambda pair: pair[1]):
    print(f"Word '{word}' count {count}")
