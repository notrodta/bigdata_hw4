import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

def group_restaurants():
    spark = SparkSession(SparkContext())

    df = spark.read.csv(path='/Users/RodTa/Desktop/spring_semester_2018/big_data/nyc_restaurants.csv',
                        sep=',',
                        encoding='UTF-8',
                        comment=None,
                        header=True,
                        inferSchema=True)

    table = df.groupBy('CUISINE DESCRIPTION').count().sort(['count'], ascending=False)

    table.show(table.count())

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    group_restaurants()
