'''
Given two tables Prodcut and Orders, Can you find out which Product was sold the most in the year 2020?

For the below dataset :
|Product|  Store|Price|
+-------+-------+-----+
| Iphone|BestBuy|  100|
| Iphone|Walmart|   90|
| Iphone| Amazon|   95|
|Samsung| Amazon|   80|
|Samsung|Walmart|   85|
|Samsung|BestBuy|   90|

Write a Query to generate the output as below without using window functions:
|Product|  Walmart_price|BestBuy_price|Amazon_price|Competitive_price|
+-------+-------+-----+
| Iphone|90|  100|  95| N
| Samsung|85|  90|   80| Y
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()
class AmazonInventory:
    def createData(self):
        data = [
            ('Iphone', 'BestBuy', 100),
            ('Iphone', 'Walmart', 90),
            ('Iphone', 'Amazon', 95),
            ('Samsung', 'Amazon', 80),
            ('Samsung', 'Walmart', 85),
            ('Samsung', 'BestBuy', 90)
        ]
        schema = ['Product', 'Store', 'Price']
        return spark.createDataFrame(data, schema)

    #SQL approach
    def priceCheck(self, inputDf):
        inputDf.createOrReplaceTempView('input')
        query = """with pivot as (
                select * from input pivot(
                max(Price) for Store in ('Walmart' as Walmart_price, 'BestBuy' as BestBuy_price, 'Amazon' as Amazon_price)
                ) ),
                minPrice as (
                select min(Price) as min_price, Product from input group by Product
                )
                select M1.*,
                case when M1.Amazon_price = M2.min_price
                    then 'Y'
                    else 'N'
                end as Competitive_price
                from pivot M1 inner join minPrice M2
                on M1.Product = M2.Product
                order by M1.Product
                """
        return spark.sql(query)

    #PySpark approach
    def amazonPriceCheck(self, inputDf):
        minPricedf = inputDf.groupBy('Product').agg(min('Price').alias('Min_Price'))
        pivotDf = inputDf.groupBy('Product').pivot('Store').max('Price')
        resultDf = pivotDf.join(minPricedf, on='Product', how='inner')\
                        .withColumn('Competitive_price', when(col('Amazon').__eq__(col('Min_Price')), lit('Y')).otherwise(lit('N')))\
                        .select('Product', col('BestBuy').alias('BestBuy_Price'), col('Walmart').alias('Walmart_Price'), col('Amazon').alias('Amazon_Price'), 'Competitive_price')\
                        .orderBy('Product')
        return resultDf

ob = AmazonInventory()
inputDf = ob.createData()

resultDf = ob.priceCheck(inputDf)
resultDf.show()

resultDf = ob.amazonPriceCheck(inputDf)
resultDf.show()