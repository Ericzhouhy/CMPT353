import sys
import re
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('wikipedia popular').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

wiki_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.IntegerType()),
    types.StructField('bytes', types.LongType()),
])

def name_to_time(pathname):
    find = re.compile('\d{8}-\d{2}')
    substring = re.search(find, pathname).group(0)
    return substring

def main(in_directory, out_directory):
    data = spark.read.csv(in_directory, schema=wiki_schema,sep=' ').withColumn('filename', functions.input_file_name())

    #English data only
    en_only_data = data.filter(data['language'] == 'en')
    #Not main page
    data_without_main = en_only_data.filter(data['title'] != 'Main_Page')
    #Not Special
    data_without_special = data_without_main.filter(data.title.startswith('Special:') == False)
    path_to_hour = functions.udf(lambda path: name_to_time(path), returnType=types.StringType())
    clean_data = data_without_special.withColumn('dates', path_to_hour(data_without_special['filename']))
    clean_data.cache()
    
    largest_views = clean_data.groupBy('dates').agg(functions.max(clean_data['views']))
    results = clean_data.join(largest_views.withColumnRenamed('max(views)', 'views'), ['dates', 'views']) 
    results = results.select('dates', 'title', 'views')
    
    results = results.sort('dates')
    
    results.write.csv(out_directory, mode='overwrite')
    
if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)