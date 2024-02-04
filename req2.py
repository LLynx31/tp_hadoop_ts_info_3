from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Requete2").getOrCreate()

customer_rdd = spark.sparkContext.textFile("Customer.txt")

# Créer un DataFrame à partir du RDD Customer
customer_df = spark.createDataFrame(customer_rdd.map(lambda line: line.split(",")), ["id", "startDate", "nom"])

# Afficher les enregistrements triés par nom
result_query2 = customer_df.orderBy("nom")
result_query2.show()
