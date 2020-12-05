package part2.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object L3_ManagingNulls  extends App{
  val spark = SparkSession.builder().config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .appName("Managing Nulls")
    .master("local[*]").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Select either rotten tomatoes or imdb rating, whichever is not null
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating") * 10,
    // Selects the first non-null value from the columns passed (in order), returns null if all columns are null
    coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating") * 10)
  ).show()

  // Checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNotNull).show() //or isNull

  // If you want to order with a column that contains nulls, you can select how to place those nulls in the order
  moviesDF.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_first) // there is also desc_nulls_last

  // Removing nulls
  // Removes rows containing nulls
  moviesDF.select("Title", "Rotten_Tomatoes_Rating").na.drop().show()
  // Replace nulls from the specified columns with some predefined value df.na.fill(predefinedValue, columns)
  moviesDF.select("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating").na.fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating")).show()
  // You can select different values for different columns by passing a map from column_name-> value
  moviesDF.na.fill(Map("Rotten_Tomatoes_Rating" -> 0, "IMDB_Rating" -> -1)).show()


  //Complex operations
  moviesDF.selectExpr("Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    //similar to coalesce
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)",
    // Same as above
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10)",
    // returns null if the two values are equal, if not returns the first value
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10)",
    // If the first argument is null,returns the second, if this is null, returns the third value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0)"
  ).show()


}
