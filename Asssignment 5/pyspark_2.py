from pyspark.sql import *
import pandas as pd
import os
import sys
if len(sys.argv) == 1:
    print("\ninvalid input format")
    print("input format: pyspark_2.py <no_of_cpu> output2.txt\n")
    exit()
no_of_cpus, outfilename = sys.argv[1:]
Cpu_str = "local["+no_of_cpus+"]"
my_spark = SparkSession.builder.master(Cpu_str).getOrCreate()
airports_data = my_spark.read.csv("airports.csv", header=True)
df = airports_data.groupby("COUNTRY").count()
df = df.toPandas()
df.sort_values(by=['count'], inplace=True, ascending=False)
df = df.values[0][0]
with open(outfilename, 'w') as file:  # Use file to refer to the file object
    file.write(df)