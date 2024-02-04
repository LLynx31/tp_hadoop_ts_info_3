from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Requete3").getOrCreate()

customer_rdd = spark.sparkContext.textFile("Customer.txt")
order_rdd = spark.sparkContext.textFile("Order.txt")

# Créer des DataFrames à partir des RDD
customer_df = spark.createDataFrame(customer_rdd.map(lambda line: line.split(",")), ["id", "startDate", "nom"])
order_df = spark.createDataFrame(order_rdd.map(lambda line: line.split(",")), ["#id", "total"])

# Effectuer une jointure entre les DataFrames
result_query3 = customer_df.join(order_df, customer_df["id"] == order_df["#id"]).select("nom", "total")
result_query3.show()
