from pyspark.sql import *
import os
import sys
if len(sys.argv) == 1:
    print("\ninvalid input format")
    print("input format: pyspark_1.py <no_of_cpu> output1.txt\n")
    exit()
no_of_cpus, outfilename = sys.argv[1:]
Cpu_str = "local["+no_of_cpus+"]"
my_spark = SparkSession.builder.master(Cpu_str).getOrCreate()
airports_data = my_spark.read.csv("airports.csv", header=True)
df = airports_data.groupby("COUNTRY").count()
df.toPandas().to_csv(outfilename, index=False)