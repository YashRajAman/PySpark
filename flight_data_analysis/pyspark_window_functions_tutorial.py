from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, rank, dense_rank, max, min, sum, avg, col

# Initialize SparkSession
spark = SparkSession.builder.appName("WindowFunctionsTutorial").getOrCreate()

# Sample DataFrame creation (replace with your actual DataFrame)
data = [
    (2020, "Jan", 100),
    (2020, "Feb", 150),
    (2020, "Mar", 120),
    (2021, "Jan", 200),
    (2021, "Feb", 180),
    (2021, "Mar", 220),
]
columns = ["Year", "Month", "Total_Flights"]
df = spark.createDataFrame(data, columns)

# Define a window specification partitioned by Year and ordered by Total_Flights descending
window_spec = Window.partitionBy("Year").orderBy(col("Total_Flights").desc())

# 1. row_number(): Assigns a unique row number within the partition
df_with_row_number = df.withColumn("row_number", row_number().over(window_spec))
df_with_row_number.show()

# 2. rank(): Assigns rank with gaps for ties
df_with_rank = df.withColumn("rank", rank().over(window_spec))
df_with_rank.show()

# 3. dense_rank(): Assigns rank without gaps
df_with_dense_rank = df.withColumn("dense_rank", dense_rank().over(window_spec))
df_with_dense_rank.show()

# 4. max() over window: Find max flights per year
window_spec_no_order = Window.partitionBy("Year")
df_with_max = df.withColumn("max_flights", max("Total_Flights").over(window_spec_no_order))
df_with_max.show()

# 5. min() over window: Find min flights per year
df_with_min = df.withColumn("min_flights", min("Total_Flights").over(window_spec_no_order))
df_with_min.show()

# 6. sum() over window: Running total of flights ordered by Month (assuming Month can be ordered)
# For demonstration, we create a window ordered by Month (string order)
window_spec_order_month = Window.partitionBy("Year").orderBy("Month").rowsBetween(Window.unboundedPreceding, 0)
df_with_running_sum = df.withColumn("running_sum", sum("Total_Flights").over(window_spec_order_month))
df_with_running_sum.show()

# 7. avg() over window: Average flights per year
df_with_avg = df.withColumn("avg_flights", avg("Total_Flights").over(window_spec_no_order))
df_with_avg.show()

# Stop SparkSession
spark.stop()
