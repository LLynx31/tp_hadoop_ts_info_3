/*// Imports
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Créer une session Spark
val spark = SparkSession.builder().master("local[3]").appName("SparkByExamples.com").config("spark.executor.memory", "3g").config("spark.executor.cores","1").config("spark.executor.instances","3").config("spark.driver.memory","2g").config("spark.driver.cores","1").getOrCreate()

// Préparer les données
val data = Array("Alui Ange Elvis", "Koulefionou Saint-Clair", "Sagoe Christian Brice-Yvan")
val rdd = spark.sparkContext.parallelize(data)

// Transformation flatMap
val rdd2 = rdd.flatMap(f => f.split(" "))

// Transformation map
val rdd3: RDD[(String, Int)] = rdd2.map(m => (m, 1))

// Transformation filter
val rdd4 = rdd3.filter(a => a._1.startsWith("a"))

// Transformation reduceByKey
val rdd5 = rdd3.reduceByKey(_ + _)

// Transformation sortByKey
val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey()

// Imprimer le résultat à la console
rdd6.foreach(println)*/

// Imports
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Créer une session Spark
val spark = SparkSession.builder().master("local[3]").appName("SparkByExamples.com").config("spark.executor.memory", "3g").config("spark.executor.cores","1").config("spark.executor.instances","3").config("spark.driver.memory","2g").config("spark.driver.cores","1").getOrCreate()

// Lire les données à partir d'un fichier texte
val rdd = spark.sparkContext.textFile("5000-8.txt")

// Transformation flatMap
val rdd2 = rdd.flatMap(f => f.split(" "))

// Transformation map
val rdd3: RDD[(String, Int)] = rdd2.map(m => (m, 1))

// Transformation filter
val rdd4 = rdd3.filter(a => a._1.startsWith("Project"))

// Transformation reduceByKey
val rdd5 = rdd3.reduceByKey(_ + _)

// Transformation sortByKey
val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey()
rdd6.collect().foreach(println)
