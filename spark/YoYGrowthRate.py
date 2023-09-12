'''

Assume you're given a table containing information about Wayfair user transactions for different products.
Write a query to calculate the year-on-year growth rate for the total spend of each product, grouping the results by product ID.

The output should include the year in ascending order, product ID, current year's spend, previous year's spend and year-on-year growth percentage, rounded to 2 decimal places.

user_transactions Table:
Column Name	Type
transaction_id	integer
product_id	integer
spend	decimal
transaction_date	datetime

user_transactions Example Input:
transaction_id	product_id	spend	transaction_date
1341	123424	1500.60	12/31/2019 12:00:00
1423	123424	1000.20	12/31/2020 12:00:00
1623	123424	1246.44	12/31/2021 12:00:00
1322	123424	2145.32	12/31/2022 12:00:00

Example Output:
year	product_id	curr_year_spend	prev_year_spend	yoy_rate
2019	123424	1500.60	NULL	NULL
2020	123424	1000.20	1500.60	-33.35
2021	123424	1246.44	1000.20	24.62
2022	123424	2145.32	1246.44	72.12


========================================================================================================================================

create table user.transaction(
transaction_id int PRIMARY KEY,
product_id int,
spend decimal,
transaction_date timestamp
);

INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1341,123424,1500.60,totimestamp(todate('2019-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1423,123424,1000.20,totimestamp(todate('2020-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1623,123424,1246.44,totimestamp(todate('2021-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1322,123424,2145.32,totimestamp(todate('2022-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1344,234412,1800.00,totimestamp(todate('2019-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1435,234412,1234.00,totimestamp(todate('2020-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(4325,234412,889.50, totimestamp(todate('2021-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(5233,234412,2900.00,totimestamp(todate('2022-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(2134,543623,6450.00,totimestamp(todate('2019-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1234,543623,5348.12,totimestamp(todate('2020-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(2423,543623,2345.00,totimestamp(todate('2021-12-31')));
INSERT INTO user.transaction(transaction_id, product_id, spend, transaction_date) VALUES(1245,543623,5680.00,totimestamp(todate('2022-12-31')));

'''

import os, sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, lag, round
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class YoyGrowthRate:
    def createData(self):
        data = [
            (1341, 123424, 1500.60, datetime(2019, 12, 31, 12, 00, 00)),
            (1423, 123424, 1000.20, datetime(2020, 12, 31, 12, 00, 00)),
            (1623, 123424, 1246.44, datetime(2021, 12, 31, 12, 00, 00)),
            (1322, 123424, 2145.32, datetime(2022, 12, 31, 12, 00, 00)),
            (1344, 234412, 1800.00, datetime(2019, 12, 31, 12, 00, 00)),
            (1435, 234412, 1234.00, datetime(2020, 12, 31, 12, 00, 00)),
            (4325, 234412, 889.50, datetime(2021, 12, 31, 12, 00, 00)),
            (5233, 234412, 2900.00, datetime(2022, 12, 31, 12, 00, 00)),
            (2134, 543623, 6450.00, datetime(2019, 12, 31, 12, 00, 00)),
            (1234, 543623, 5348.12, datetime(2020, 12, 31, 12, 00, 00)),
            (2423, 543623, 2345.00, datetime(2021, 12, 31, 12, 00, 00)),
            (1245, 543623, 5680.00, datetime(2022, 12, 31, 12, 00, 00))
        ]

        column = ['transaction_id', 'product_id', 'spend', 'transaction_date']
        return spark.createDataFrame(data, column)

    def getYoyGrowthDf(self, inputDf):
        spendDf = inputDf.select(year('transaction_date').alias('year'),
                                  'product_id',
                                  col('spend').alias('curr_year_spend'),
                                  lag('spend', 1).over(Window.partitionBy('product_id').orderBy('transaction_date')).alias('prev_year_spend')
                                  ).orderBy('product_id','year')

        return spendDf.withColumn('yoy_rate', round((col('curr_year_spend')-col('prev_year_spend'))*100/col('prev_year_spend'),2))


rate = YoyGrowthRate()
inputDf = rate.createData()
yoyDf = rate.getYoyGrowthDf(inputDf)
yoyDf.show()