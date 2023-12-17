'''
A U.S graduate school has students from Asia, Europe and America. The students’ location information are stored in table student as below.

| name   | continent |
|--------|-----------|
| Jack   | America   |
| Pascal | Europe    |
| Xi     | Asia      |
| Jane   | America   |

Pivot the continent column in this table so that each name is sorted alphabetically and displayed underneath its corresponding continent.
The output headers should be America, Asia and Europe respectively.
It is guaranteed that the student number from America is no less than either Asia or Europe (or any other).
For the sample input, the output is:

| America | Asia | Europe |
|---------|------|--------|
| Jack    | Xi   | Pascal |
| Jane    |      |        |

Follow-up: If it is unknown which continent has the most students, can you write a query to generate the student report?
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, first
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class StudentReport:
    def createData(self):
        data = [
            ('Jack', 'America'),
            ('Pascal', 'Europe'),
            ('Xi', 'Asia'),
            ('Jane', 'America'),
            ('Robert', 'Australia'),
            ('Shane', 'Australia'),
            ('Matthew', 'Australia')
        ]
        schema = ['name', 'continent']
        return spark.createDataFrame(data, schema)

    def report(self, inputDf):
        inputDf.createOrReplaceTempView('student')
        # Collect distinct continents
        continents = inputDf.select("continent").distinct().sort('continent').collect()

        query = f"""with cte as ( select name, continent, 
                row_number() over(partition  by continent order by name) as rnum from student ) 
                select {', '.join([f'{cont.continent}' for cont in continents])}
                from cte pivot(first(name) for continent in ({', '.join([f"'{cont.continent}'" for cont in continents])})) 
                """
        resultDf = spark.sql(query)
        return resultDf

    def pivotReport(self, df):
        # Add row number within each continent
        window_spec = Window().partitionBy("continent").orderBy("name")
        df = df.withColumn("row_number", row_number().over(window_spec))

        # Pivot the DataFrame
        pivoted_df = df.groupBy("row_number").pivot("continent").agg(first("name")).orderBy("row_number").drop("row_number")

        # Rename the columns, if required
        #continents = df.distinct().select('continent').collect()
        #for cont in continents:
        #    pivoted_df = pivoted_df.withColumnRenamed(cont.continent, f"{cont.continent}")
        return pivoted_df


ob = StudentReport()
inputDf = ob.createData()

resultDf = ob.report(inputDf)
resultDf.show()

resultDf = ob.pivotReport(inputDf)
resultDf.show()