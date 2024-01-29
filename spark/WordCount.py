'''
We have a file *.txt at input path. Find out the frequency of each word exists in text file.

"In a world full of #technology, understanding data is key to success. Data analytics, AI, and machine learning are transforming industries.
Companies are racing to harness the power of data-driven insights; But,  data is messy and comes in various formats - structured, unstructured, and semi-structured.
The challenge is to clean, process, and  analyze this data effectively. There's a growing demand for data scientists, analysts, and engineers who can unlock the value hidden within the data."

words   count
a: 2
ai: 1
analyze: 1
and: 2
are: 1
challenge: 1
clean: 1
comes: 1
companies: 1
data: 4
data-driven: 1
demand: 1
effectively: 1
engineers: 1
for: 1
formats: 1
full: 1
growing: 1
harness: 1
hidden: 1
in: 2
industries: 1
is: 1
key: 1
learning: 1
machine: 1
messy: 1
of: 2
power: 1
process: 1
racing: 1
semi-structured: 1
structured: 1
success: 1
technology: 1
the: 1
the: 2
this: 1
to: 1
transforming: 1
understanding: 1
unstructured: 1
unlock: 1
value: 1
various: 1
who: 1
within: 1
world: 1


✅ 𝗘𝘅𝗽𝗹𝗮𝗻𝗮𝘁𝗶𝗼𝗻:
1. Analyze all the special character involved in input file data.
2. Use regexp replace to replace these characters with space.
3. Convert the data into lower case and split on space.
4. Use explode to convert the result into rows.
5. Aggregate each word count and filter the blank values
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, explode, count, lower

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class WordCount:
    def createData(self):
        line = """In a world full of #technology, understanding data is key to success. Data analytics, 'AI', and machine learning are transforming industries. Companies are racing to harness the power of data-driven insights; But,  data is messy and comes in various formats - structured, unstructured, and semi-structured. The challenge is to clean, process, and  analyze this data effectively. There's a growing demand for data scientists, analysts, and engineers who can unlock the value hidden within the data."""

        dataDf = spark.createDataFrame([(line,)], ['text'])
        return dataDf

    def wCount(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = """with cte as (
                select regexp_replace(text, "[';.,#*-]", ' ') as text
                from table),
                M1 as (select 
                explode(split(lower(text), ' ')) as words 
                from cte)
                select 
                words, count(1)
                from M1 
                where words != '' 
                group by words 
                order by words"""
        return spark.sql(query)

    def wordCount(self, inputDf):
        pattern = "[';.,#*-_]"
        replacement = ' '
        regexDf = inputDf.select(regexp_replace(col('text'), pattern, replacement).alias('words'))
        splitDf = regexDf.select(explode(split(lower('words'), ' ')).alias('words'))
        countDf = splitDf.groupBy('words').agg(count('*').alias('counts'))\
                        .filter(col('words') != '').orderBy('words')
        return countDf


ob = WordCount()
inputDf = ob.createData()
resultDf = ob.wordCount(inputDf)
resultDf.show(100, False)

resultDf = ob.wCount(inputDf)
resultDf.show(100, False)