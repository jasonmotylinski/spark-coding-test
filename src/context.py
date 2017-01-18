"""The Spark and SQL contexts referenced by other modules."""
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sql = SQLContext(sc)
