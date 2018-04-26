import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

def group_restaurants():
    spark = SparkSession(SparkContext())

    df = spark.read.csv('nyc_restaurants.csv',
                      header=True,
                      inferSchema=True)

    table = df.groupBy('CUISINE DESCRIPTION').count().sort(['count'], ascending=False)

    table.show(table.count())

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    group_restaurants()
