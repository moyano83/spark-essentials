package part4.rdds

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object L1_RDDs extends App{
  val spark = SparkSession.builder().master("local[*]").appName("Intro to RDDs").getOrCreate()
  // To create RDDs we need a spark context
  val sc = spark.sparkContext

  // Ways to create an RDD
  // 1 - Parallelize a collection
  val numbersRDD = sc.parallelize(1 to 10000)
  // 2 - Read from files
  case class Stock (symbol:String, date:String, price:Double)
  def readStocks(filename:String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1) // removing the header
      .map(line=> line.split(","))
      .map(tokens=> Stock(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - Read from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .filter(line => line != "symbol,date,price")
    .map(line=> line.split(","))
    .map(tokens=> Stock(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - Read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  // We need to transform it to a DS and then to an RDD
  val stocksDS = stocksDF.as[Stock]
  val stocksRDD3 = stocksDS.rdd

  // Transforming RDDs to DF
  val numbersDF = numbersRDD.toDF("numbers") // Type information is lost in this transformation
  // Transforming RDDs to DS
  val numbersDS = spark.createDataset(numbersRDD) // Type information is preserved

  // Transformations
  // Count -> Eager, filter -> lazy
  val microsoftCount = stocksRDD.filter(_.symbol == "MSFT").count()

  // also lazy
  val cpNames = stocksRDD.map(_.symbol).distinct()

  // min and max (we need to define an implicit ordering)
  implicit val stockOrdering = Ordering.fromLessThan[Stock]((a,b)=>a.price < b.price)
  val minSft = stocksRDD.min()
  val maxSft = stocksRDD.max()

  // reduce
  numbersRDD.reduce((a,b) => a+b)

  // Grouping (stocks by symbol)
  val groupedStocksRdd = stocksRDD.groupBy(_.symbol) // expensive operation involves shuffles

  // Partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30) // returns a new RDD of stocks
  // This op would save 30 files in the provided route, it is expensive as it involves shuffling
  // Best practice: repartition early, then process the results. The size of a partition should be between 10-100 MB
  repartitionedStocksRDD.toDF().write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks30")

  // Coalescing is repartition by fewer partitions that the RDD currently has
  val coalesceStocksRDD = repartitionedStocksRDD.coalesce(10) // Does not involve shuffling
  coalesceStocksRDD.toDF().write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks10")

  /**
    * Exercises:
    * 1 - Read the movies json as an rdd into the case class below
    */
  case class Movie(title:String, genre:String, rating:Double)
  import spark.implicits._
  val moviesRDD = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull.and(col("rating").isNotNull))
    .as[Movie]
    .rdd
  /**
    * 2 - Show the distinct genres as an RDD
    */
  val genresRDD = moviesRDD.map(_.genre).distinct()
  /**
    * 3 - Select all the movies in the Drama genre with a rating > 6
    */
  val dramaMoviesRDD = moviesRDD.filter(m => m.genre=="Drama" && m.rating > 6.0)
  /**
    * 4 - Show the avg rating of movies by genre
    */
  val avgGenreRDD = moviesRDD.groupBy(_.genre).map{ case (genre, movies) => {
      val res = movies.foldLeft((0,0.0))((tup, mov) => (tup._1 + 1, tup._2 + mov.rating))
      (genre, res._2 / res._1)
    }
  }

  genresRDD.toDF.show()
  dramaMoviesRDD.toDF.show()
  avgGenreRDD.toDF.show()

}
