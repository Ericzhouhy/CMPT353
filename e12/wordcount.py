import sys
from pyspark.sql import SparkSession, functions, types
import string, re
import math

spark = SparkSession.builder.appName('wordcount').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

def main(in_directory, out_directory):
    text = spark.read.text(in_directory).cache()
    wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation
    text = text.withColumn("value", functions.split("value", wordbreak)).cache()
    text = text.select(functions.explode(text.value).alias('word'))
    text = text.select(functions.lower(functions.col('word')).alias('word'))
    text = text.filter(text['word'] != '')
    text = text.groupby('word').agg(functions.count('word').alias('count'))
    text = text.sort(functions.desc('count'), functions.asc('word'))
    #text.show()
    text.write.csv(out_directory, mode='overwrite')

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)