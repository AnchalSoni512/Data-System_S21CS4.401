from pyspark.sql import *
import os
import sys
import pandas as pd
if len(sys.argv) == 1:
    print("\ninvalid input format")
    print("input format: pyspark_3.py <no_of_cpu> output3.txt\n")
    exit()
no_of_cpus, outfilename = sys.argv[1:]
Cpu_str = "local["+no_of_cpus+"]"
my_spark = SparkSession.builder.master(Cpu_str).getOrCreate()
airports_data = my_spark.read.csv("airports.csv", header=True)
df = airports_data.filter((airports_data.LATITUDE >= 10) & (airports_data.LATITUDE <= 90) & (airports_data.LONGITUDE <= -10) & (airports_data.LONGITUDE >= -90))
df = df.toPandas().iloc[:,2]
df.to_csv(outfilename, index=False)
