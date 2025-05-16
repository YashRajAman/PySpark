from pyspark import SparkContext, SparkConf
from email import header
from pyspark.sql import SparkSession


conf = (SparkConf().setMaster("local").setAppName("Flight Data Analysis").set("spark.executro.memory", "2g"))

sc = SparkContext(conf=conf) #SparkContext(master='local[*]', appName="Flight Data Analysis")

spark = SparkSession(sc) #SparkSession.builder.appName("Flight Data Analysis").getOrCreate()

#=======================================
#Loading data using auto schema inference

# datadf = spark.read.options(header=True).csv("/home/aman/programs/gitrepos/PySpark/flight_data_analysis/Airports2.csv")
# datadf.printSchema()
# datadf.show()

#=======================================
#Defining schema to load data using the provided schema instead of auto inference of schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, NumericType, DateType, LongType

schema = StructType([StructField("Origin_airport",StringType(), True),
                     StructField("Destination_airport", StringType(), True),
                     StructField("Origin_city", StringType(), True),
                     StructField("Destination_city", StringType(), True),
                     StructField("Passengers", IntegerType(), True),
                     StructField("Seats", IntegerType(), True),
                     StructField("Flights", IntegerType(), True),
                     StructField("Distance", LongType(), True),
                     StructField("Fly_date", DateType(), True),
                     StructField("Origin_population", LongType(), True),
                     StructField("Destination_population", LongType(), True),
                     StructField("Org_airport_lat", StringType(), True),
                     StructField("Org_airport_long", StringType(), True),
                     StructField("Dest_airport_lat", StringType(), True),
                     StructField("Dest_airport_long", StringType(), True),
                     
                     ])

dataframe = spark.read.options(header=True, nullValue='NA').csv("/home/aman/programs/gitrepos/PySpark/flight_data_analysis/Airports2.csv", schema=schema)

print("Total row count: ", dataframe.count())
dataframe.printSchema()
dataframe.show(5)


sc.stop()
#=======================================
#Dataframe operations
"""
Origin and Destination Airports:

What are the most frequent origin and destination airports in the dataset?
How many unique origin and destination airports are there?
"""

print("Most frequent origin airports:")
dataframe.select("Origin_airport").distinct().count()
dataframe.select("Destination_airport").distinct().count()



"""
Cities:

Which city pairs (origin and destination) have the highest number of flights?
What is the most common origin city for flights?
Passengers:

What is the total number of passengers for all flights in the dataset?
Which route (origin to destination) has the highest number of passengers?
Seats:

What is the average number of seats available per flight?
Which flight route has the highest seat capacity?
Flights:

How many flights are there in total?
What is the average number of flights between two cities?
Distance:

What is the longest and shortest flight distance in the dataset?
Which route has the longest distance?
Fly Date:

What is the range of dates covered in the dataset?
How many flights occurred on a specific date (e.g., 2008-10-01)?
Population:

What is the average population of origin and destination cities?
Which city has the highest population as an origin or destination?
Latitude and Longitude:

Are there any missing latitude or longitude values for airports?
What is the geographical distribution of origin and destination airports?
Combined Questions:

Which city pair has the highest number of passengers and the longest distance?
Are there any correlations between the number of passengers and the population of the origin or destination city?


"""