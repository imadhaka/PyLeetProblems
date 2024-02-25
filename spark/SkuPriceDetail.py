"""
𝐅𝐢𝐧𝐝 𝐭𝐡𝐞 𝐏𝐫𝐢𝐜𝐞 𝐚𝐭 𝐬𝐭𝐚𝐫𝐭 𝐨𝐟 𝐭𝐡𝐞 𝐦𝐨𝐧𝐭𝐡 𝐚𝐧𝐝 𝐭𝐡𝐞 𝐝𝐢𝐟𝐟𝐞𝐫𝐞𝐧𝐜𝐞 𝐢𝐧 𝐭𝐡𝐞 𝐩𝐫𝐢𝐜𝐞 𝐟𝐫𝐨𝐦 𝐩𝐫𝐞𝐯𝐢𝐨𝐮𝐬 𝐦𝐨𝐧𝐭𝐡 𝐟𝐨𝐫 𝐞𝐚𝐜𝐡 𝐬𝐤𝐮_𝐢𝐝.

data = [
(1, '2023-01-01', 10),
(1, '2023-01-27', 13),
(1, '2023-02-01', 14),
(1, '2023-02-15', 15),
(1, '2023-03-03', 18),
(1, '2023-03-27', 15),
(1, '2023-04-06', 20),
(2, '2024-01-01', 50),
(2, '2024-01-29', 100),
(2, '2024-02-01', 150)
]
schema = "sku_id int , price_date string , price int"

Output--
|sku_id|price_date|price|price_diff|
+------+----------+-----+----------+
|     1|2023-01-01|   10|      null|
|     1|2023-02-01|   14|         4|
|     1|2023-03-01|   15|         1|
|     1|2023-04-01|   15|         0|
|     1|2023-05-01|   20|         5|
|     2|2024-01-01|   50|      null|
|     2|2024-02-01|  150|       100|
|     2|2024-04-01|  150|         0|
"""
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class SkuPriceDetail:
    def createData(self):
        data = [
            (1, '2023-01-01', 10),
            (1, '2023-01-27', 13),
            (1, '2023-02-01', 14),
            (1, '2023-02-15', 15),
            (1, '2023-03-03', 18),
            (1, '2023-03-27', 15),
            (1, '2023-04-06', 20),
            (2, '2024-01-01', 50),
            (2, '2024-01-29', 100),
            (2, '2024-02-01', 150)
        ]
        schema = ['sku_id', 'price_date' , 'price']
        return spark.createDataFrame(data, schema)

    def getPriceDiff(self, inputDf):
        inputDf.createOrReplaceTempView("table")
        query = """with RankedPrices AS (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY price_date desc) AS row_num
            FROM table
            ),
            date as (
            select sku_id, price,
            CASE WHEN DAY(price_date) > 1
                THEN DATE_FORMAT(ADD_MONTHS(price_date, 1), 'yyyy-MM-01')
                ELSE DATE_FORMAT(price_date, 'yyyy-MM-01')
            END AS price_month
            from table
            union all 
            select sku_id, price,
            DATE_FORMAT(ADD_MONTHS(price_date, 1), 'yyyy-MM-01') as price_month
            from RankedPrices
            where DAY(price_date) = 1 and row_num = 1
            ),
            result as (
            select sku_id, 
            price_month as price_date, 
            last(price) as price
            from date
            group by sku_id, price_month
            )
            select sku_id, price_date, price,
            price - LAG(price) OVER (PARTITION BY sku_id ORDER BY price_date) AS price_diff
            from result
            order by sku_id, price_date
            """

        return spark.sql(query)

    def getPriceDiffPy(self, inputDf):
        inputDf = inputDf.withColumn('rnum', row_number().over(Window.partitionBy('sku_id').orderBy(desc('price_date'))))
        dateDf = inputDf.withColumn('price_month', when(dayofmonth('price_date').__gt__(1), date_format(add_months('price_date', 1), 'yyyy-MM-01'))
                                                        .otherwise(date_format('price_date', 'yyyy-MM-01')))
        dateDf = inputDf.filter(dayofmonth('price_date').__eq__(1) & col('rnum').__eq__(1))\
                    .withColumn('price_date', date_format(add_months('price_date', 1), 'yyyy-MM-01'))\
                    .withColumn('price_month', col('price_date'))\
                .unionAll(dateDf)
        priceDf = dateDf.groupBy(col('sku_id'), col('price_month').alias('price_date')).agg(last('price').alias('price'))
        return priceDf.withColumn('price_diff', col('price')-lag('price').over(Window.partitionBy('sku_id').orderBy('price_date'))).orderBy('sku_id', 'price_date')

ob = SkuPriceDetail()
dataDf = ob.createData()

resultDf = ob.getPriceDiff(dataDf)
resultDf.show()

resultDf = ob.getPriceDiffPy(dataDf)
resultDf.show()