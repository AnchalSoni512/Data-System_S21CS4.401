# Problem statement:
### To solve simple data querying problems using Spark architectures.
- Write a program to get the number of Airports by Country.
- Write a program to find the Country having the highest number of airports.
- Write a program to find airports whose latitude is between [10, 90] and longitude is between [-10, -90]. ([a,b] a,b both are included)

# Dataset:
- Dataset for the problem is a dataset on Airports(airports.csv)

# Install dependencies:
- pip install pyspark

# To run:
- `python pyspark_1.py <no_of_cpus> output1.txt`
- `python pyspark_2.py <no_of_cpus> output2.txt`
- `python pyspark_3.py <no_of_cpus> output3.txt`
<no_of_cpus> : int value which denotes the number of cpus.
outputtxt : denotes the name of the output file to write output.
Note : Headers are included in the output file.