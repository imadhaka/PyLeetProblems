'''
You have a table with 3 columns: cricket ( delivery_no , runs_scored, delivery_type)

delivery type can be either of 3:
legal
nb
wd

Assume in an ODI cricket match there were 10 extra balls (no ball or wide) . So there will be 310 records in the table.
You need to find runs scored in every over. so there will be 50 records in the output.
over_no , runs_scored


Few things to remember
1- there can be 2 or more consecutive extra deliveries eg : 2 consecutive wide balls.
2- runs can be scored on extra deliveries as well. eg: a boundary on a no ball or a wide ball. So a total of 5 runs would be counted in that case.
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, ceil, min, max, lit, sum, lag, least
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class CricketScorecard:
    def createData(self):
        return spark.read.option('header',True).csv('C:/Users/thean/PycharmProjects/PyLeetProblems/data/cricket.csv')

    def getOverRuns(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = """with legal as (
                select int(delivery_no) as delivery_no, runs_scored, delivery_type,
                ceil(row_number() over(order by int(delivery_no)) / 6) as over
                from table
                where delivery_type = 'legal'),
                
                overDelivery as (
                select
                over,
                first_ball,
                last_ball,
                lag(last_ball) over(order by over) + 1 as prev_over_last_del
                from (
                    select over,
                    min(delivery_no) as first_ball,
                    max(delivery_no) as last_ball
                    from legal group by over
                    ) M1
                ),
                
                notLegal as (
                select o.over,
                int(t.runs_scored) + 1 as runs_scored
                from table t join overDelivery o
                on t.delivery_no BETWEEN least(o.first_ball, o.prev_over_last_del) AND o.last_ball
                where t.delivery_type != 'legal'
                ) 
                
                select over, sum(runs_scored) as runs_scored
                from (select over, runs_scored from legal union all select over, runs_scored from notLegal) M1
                group by over order by over
                """
        return spark.sql(query)

    def runs(self, cricket_data):
        cricket_data = cricket_data.withColumn('delivery_no', lit(col('delivery_no')).cast('int'))
        # Filter legal deliveries and apply window function to Calculate overs based on 6 legal deliveries
        oversDf = cricket_data.filter("delivery_type = 'legal'").withColumn('over', ceil(row_number().over(Window.orderBy('delivery_no'))/6))

        # Find the first and last ball of each over
        deliveryDf = oversDf.groupBy('over').agg(min('delivery_no').alias('first_ball'), max('delivery_no').alias('last_ball'))
        deliveryDf = deliveryDf.withColumn('prev_over_last_del', lag('last_ball').over(Window.orderBy('over')) + 1)

        # Calculate overs for extra deliveries
        extrasDf = cricket_data.filter("delivery_type != 'legal'").alias('A')\
                                .join(deliveryDf.alias('B'), col('A.delivery_no').between(least(col('B.first_ball'), col('prev_over_last_del')), col('B.last_ball')))\
                                .select('B.over', (col("A.runs_scored").cast("int") + 1).alias("runs_scored"))

        resultDf = oversDf.select('over', 'runs_scored').unionAll(extrasDf)\
                        .groupBy('over').agg(sum('runs_scored').alias('runs_scored'))\
                        .orderBy('over')

        return resultDf

obj = CricketScorecard()
inputDf = obj.createData()

resultDf = obj.getOverRuns(inputDf)
resultDf.show()

resultDf = obj.runs(inputDf)
resultDf.show()