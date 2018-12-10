import sys
import argparse
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":


  parser = argparse.ArgumentParser()
  parser.add_argument("-i", "--kuyil_test", type = str, required=True, help = "input path for request logs")
  parser.add_argument("-m", "--output", type = str, required=True, help = "input path for meta logs")
  args = parser.parse_args()    
  
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Word count in pyhton")
  sc = SparkContext(conf=conf)
 
  
  # read in text file and split each document into words
  words = sc.textFile(args.kuyil_test).flatMap(lambda line: line.split(" "))
 
  # count the occurrence of each word
  wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
 
  wordCounts.saveAsTextFile(args.output)
