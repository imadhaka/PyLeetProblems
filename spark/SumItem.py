"""
Input DataSet
name  item    weight
john  tamato   2
bill  apple    2
john  banana   2
john  tamato   3
bill  taco     2
bill  apple    2


Output DataSet
john (tamato,5),(banana,2)
bill (apple,4),(taco,2)
"""

import os
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import collect_list, struct

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[*]").getOrCreate()

class SumItem:
    # Approach 1 to create Dataframe
    def getDataframe(self):
        df = spark.createDataFrame([
            Row(name="John", item="tomato", weight=2),
            Row(name="Bill", item="apple", weight=2),
            Row(name="John", item="banana", weight=2),
            Row(name="John", item="tomato", weight=3),
            Row(name="Bill", item="taco", weight=2),
            Row(name="Bill", item="apple", weight=2)
        ])
        return df

    # Approach 2 to create dataframe
    def getDataframe2(self):
        data = [
            ("john", "tomato", 2),
            ("bill", "apple", 2),
            ("john", "banana", 2),
            ("john", "tomato", 3),
            ("bill", "taco", 2),
            ("bill", "apple", 2)
        ]

        # Create a DataFrame from the input data
        columns = ["name", "item", "weight"]
        df = spark.createDataFrame(data, columns)
        return df

    # Appraoch 1 to get the output
    def sumWeight(self, df):
        df = df.groupBy("name", "item").sum("weight")
        resultDf = df.groupBy("name").agg(collect_list(struct("item", "sum(weight)")).alias("items"))
        return resultDf

    def sumWeight2(self, df):
        df = df.groupBy("name", "item").agg({"weight": "sum"})
        resultDf = df.groupBy("name").agg(collect_list(struct("item", "sum(weight)")).alias("items"))
        return resultDf

# crate class object
ob = SumItem()

# Executing Approach 1
inputDf = ob.getDataframe()
sumDf = ob.sumWeight(inputDf)
sumDf.show(truncate=False)

# Executing Approach 2
inputDf = ob.getDataframe2()
sumDf = ob.sumWeight2(inputDf)
sumDf.show(truncate=False)
