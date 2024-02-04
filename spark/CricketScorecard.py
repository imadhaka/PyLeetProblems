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
from pyspark.sql.functions import col, row_number, ceil, min, max, lit, sum, lag, least, coalesce, when
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class CricketScorecard:
    def createData(self):
        return spark.read.option('header',True).csv('C:/Users/thean/PycharmProjects/PyLeetProblems/data/cricket.csv')

    def getOverRuns(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = """with input as (
                select int(delivery_no) as delivery_no,
                runs_scored, delivery_type
                from table
                ),
                
                legal as (
                select delivery_no, runs_scored,
                ceil(row_number() over(order by delivery_no) / 6) as over
                from input
                where delivery_type = 'legal'),
                
                overDelivery as (
                select
                over,
                first_ball,
                last_ball,
                lag(last_ball, 1, 0) over(order by over) + 1 as prev_over_last_del
                from (
                    select over,
                    min(delivery_no) as first_ball,
                    max(delivery_no) as last_ball
                    from legal group by over
                    ) M1
                ),
                
                extras as (
                select o.over,
                int(if(t.runs_scored = 'W', 0, t.runs_scored)) + 1 as runs_scored
                from input t join overDelivery o
                on t.delivery_no BETWEEN least(o.first_ball, o.prev_over_last_del) AND o.last_ball
                where t.delivery_type != 'legal'
                ),
                
                lastDelivery as (
                select
                if(l.max_delivery_no = 6,  max_over + 1, max_over) as over,
                sum(coalesce(int(t.runs_scored), 0)) + 1 as runs_scored
                from input t JOIN (SELECT MAX(last_ball) AS max_delivery_no, max(over) as max_over FROM overDelivery) l
                ON t.delivery_no > l.max_delivery_no
                group by l.max_over, l.max_delivery_no
                )
                
                select over, sum(runs_scored) as runs_scored
                from (select over, runs_scored from legal union all 
                        select over, runs_scored from extras union all 
                        select over, runs_scored from lastDelivery
                    ) M1
                group by over order by over
                
                """
        return spark.sql(query)

    def runs(self, cricket_data):
        cricket_data = cricket_data.withColumn('delivery_no', lit(col('delivery_no')).cast('int'))
        # Filter legal deliveries and apply window function to Calculate overs based on 6 legal deliveries
        oversDf = cricket_data.filter("delivery_type = 'legal'").withColumn('over', ceil(row_number().over(Window.orderBy('delivery_no'))/6))

        # Find the first and last ball of each over
        deliveryDf = oversDf.groupBy('over').agg(min('delivery_no').alias('first_ball'), max('delivery_no').alias('last_ball'))
        deliveryDf = deliveryDf.withColumn('prev_over_last_del', lag('last_ball', 1, 0).over(Window.orderBy('over')) + 1)

        # Calculate overs for extra deliveries
        extrasDf = cricket_data.filter("delivery_type != 'legal'").alias('A')\
                                .join(deliveryDf.alias('B'), col('A.delivery_no').between(least(col('B.first_ball'), col('prev_over_last_del')), col('B.last_ball')))\
                                .select('B.over', (coalesce(col("A.runs_scored").cast("int"), lit(0)) + 1).alias("runs_scored"))

        # Calculate runs if innings last ball is extra
        maxOverDf = deliveryDf.agg(max('last_ball').alias('max_delivery'), max('over').alias('max_over'))
        lastDeliveryDf = cricket_data.join(maxOverDf, col('delivery_no') > col('max_delivery'))\
                        .groupBy('max_delivery', 'max_over').agg((sum(coalesce(col("runs_scored").cast("int"), lit(0)))+1).alias('runs_scored'))\
                        .withColumn('over', when(col('max_delivery').__eq__(6), col('max_over')+1).otherwise(col('max_over')))\
                        .select('over', 'runs_scored')

        resultDf = oversDf.select('over', 'runs_scored').unionAll(extrasDf).unionAll(lastDeliveryDf)\
                        .groupBy('over').agg(sum('runs_scored').alias('runs_scored'))\
                        .orderBy('over')

        return resultDf

obj = CricketScorecard()
inputDf = obj.createData()

resultDf = obj.getOverRuns(inputDf)
resultDf.show()

resultDf = obj.runs(inputDf)
resultDf.show()