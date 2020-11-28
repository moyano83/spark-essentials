package part1.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import L3_ColumnsAndExpressions.spark

object L4_Aggregations extends App{
  // Creating a spark session
  val spark = SparkSession
    .builder()
    .appName("Aggregations and grouping")
    .master("local[*]")
    .getOrCreate()

  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")

  // Various types of aggregations
  // Counting
  val genres = moviesDF.select(count(col("Major_Genre"))) // This counts all values that are not null
  genres.show()
  // alternative way to do this
  val genres2 = moviesDF.selectExpr("count(Major_Genre)")
  //If we want to count the rows including the nulls, we can do something like moviesDF.select(count("*"))
  genres2.show()
  // Counting distinct
  val genresDistinct = moviesDF.select(countDistinct(col("Major_Genre")))
  genresDistinct.show()

  // Approx count distinct
  val genresApproxDistinct = moviesDF.select(approx_count_distinct(col("Major_Genre")))
  genresApproxDistinct.show()

  // min and max
  val minRatingIMDB = moviesDF.select(min(col("IMDB_Rating")))
  minRatingIMDB.show()
  val maxRatingIMDB = moviesDF.select(max(col("IMDB_Rating")))
  maxRatingIMDB.show()

  //sum
  val sumGross = moviesDF.select(sum(col("US_Gross")))
  val sumGross2 = moviesDF.selectExpr("sum(US_Gross)")
  sumGross.show()

  //avg
  val avgRating = moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  val avgRating2 = moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")
  avgRating.show()

  // data science functions
  val stdAndMeanRating = moviesDF.select(mean(col("Rotten_Tomatoes_Rating")), stddev(col("Rotten_Tomatoes_Rating")))
  stdAndMeanRating.show()

  /**
    * GROUPING
    */
  // Imagine we want to compute the number of movies per genre
  //equivalent to: select count(*) from movies group by Major_Genre
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")).count()
  countByGenreDF.show()

  // Average by genre
  val avgByGenreDF = moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Votes")
  avgByGenreDF.show()

  // Equivalent to
  val aggByGenre = moviesDF.groupBy(col("Major_Genre"))
    .agg(
      count("*").as("nr_movies"),
      avg("IMDB_Votes").as("AVG_Rating"),
      stddev("IMDB_Votes")
    ).orderBy("AVG_Rating")

  /**
    * Exercise:
    * 1 - Sum up all the profits from all the movies in the DF
    * 2 - Count how many distinct directors there is in the DF
    * 3 - Show the mean and stddev of us_gross_revenue for the movies
    * 4 - Compute the avg IMDB rating and the avg us gross revenue per director and sort it by the most profitable
    */
  // 1
  moviesDF.select(sum(col("Worldwide_Gross")).as("Total_Gross")).show()
  // 2
  moviesDF.select(countDistinct(col("Director")).as("Different Directors")).show()
  // 3
  moviesDF.select(mean(col("US_Gross")).as("Mean_Gross"), stddev(col("US_Gross")).as("STDDEV_Gross")).show()
  // 4
  moviesDF.groupBy("Director").agg(avg("IMDB_Rating").as("Average Rating"),
      sum(col("US_Gross")).as("Total Gross")).orderBy(desc("Average Rating")).show()

}
