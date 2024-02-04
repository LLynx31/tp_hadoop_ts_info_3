from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Requete4").getOrCreate()

customer_rdd = spark.sparkContext.textFile("Customer.txt")
order_rdd = spark.sparkContext.textFile("Order.txt")

# Créer des DataFrames à partir des RDD
customer_df = spark.createDataFrame(customer_rdd.map(lambda line: line.split(",")), ["id", "startDate", "nom"])
order_df = spark.createDataFrame(order_rdd.map(lambda line: line.split(",")), ["#id", "total"])

# Convertir le champ "total" en entier (int)
order_df = order_df.withColumn("total", order_df["total"].cast("int"))

# Effectuer une jointure entre les DataFrames
result_query4 = customer_df.join(order_df, customer_df["id"] == order_df["#id"]).groupBy("nom").avg("total")
result_query4.show()

