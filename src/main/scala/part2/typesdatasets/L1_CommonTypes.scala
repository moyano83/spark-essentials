package part2.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object L1_CommonTypes extends App {
  // Creating a spark session
  val spark = SparkSession
    .builder()
    .appName("Common Types")
    .master("local[*]")
    .getOrCreate()

  val moviesDF = spark.read.format("json").option("inferSchema", "true").load("src/main/resources/data/movies.json")

  // Adding a plain value to a DF
  moviesDF.select(col("title"), lit(50).as("Plain_value")).show()

  // BOOLEANS
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  // Chain of boolean filters using 'and' function. 'or' also available. This is an expression, so it can be used as a col
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select(col("title")).where(preferredFilter).show()

  // PreferredFilter is a boolean  column
  val goodMoviesDF = moviesDF.select(col("title"), preferredFilter.as("Good_Movie"))
  goodMoviesDF.show()
  // The result can be further filtered
  goodMoviesDF.where(col("Good_Movie")).show() // this can also be negated as `where(not(col("Good_Movie")))`

  // NUMBERS
  // Math operators
  val moviesDFAvgRatings = moviesDF
    .select(col("title"), ((col("Rotten_Tomatoes_Rating") / 10) + col("IMDB_Rating")) / 2)

  moviesDFAvgRatings.show() // If cols are not numeric, spark will crash

  // Find correlations between two values:
  // corr is an action, so it returns a value between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // STRINGS
  val carsDF = spark.read.format("json").option("inferSchema", "true").load("src/main/resources/data/cars.json")
  // A common function to process string is capitalization
  // First letter capitalization with initcap, lower and upper, and contains
  carsDF.select(initcap(col("Name")), lower(col("Origin"))).where(col("Name").contains("citroen")).show()

  // It is also possible to use regex
  val regex = "volkswagen|vw"
  // the groupIdx parameter (int) is the id of the match sequence, we are just interested in the first one so we pass 0
  carsDF.select(col("Name"), regexp_extract(col("Name"), regex,0).as("regex_extract"))
    .where(col("regex_extract") =!= "").show()

  // It is possible to substitute a string using regex as well
  carsDF.select(col("Name"), regexp_replace(col("Name"), regex, "People's car").as("Type"))
    .where(col("Type") =!= "").show()

  /**
    * Exercise:
    * - Filter the cars DF by the list of cars names obtain by an API call. Do it with contains and regex
    */
  def getCarNames:List[String]= List("Volkswagen", "Mercedes-Benz", "Ford")

  // Regex
  val complexRegex = getCarNames.map(_.toLowerCase).mkString("|")
  carsDF
    .select(col("Name"), regexp_extract(col("Name"), complexRegex, 0).as("isIn"))
     .where(col("isIn") =!= "").show()

  // Contains
  carsDF.filter(getCarNames.map(_.toLowerCase).map(col("Name").contains).fold(lit(false))((a,b) =>a or b)).show()
}
