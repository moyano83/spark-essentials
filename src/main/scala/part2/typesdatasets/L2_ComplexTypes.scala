package part2.typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

object L2_ComplexTypes extends App{
  val spark = SparkSession.builder().config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .appName("Complex DataTypes")
    .master("local[*]").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // DATES
  // What to do if you want to operate on dates if your DF has the column as a string
  val moviesWithDate =
    moviesDF.select(col("title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
  moviesWithDate.show()


  // When spark fails to parse a date, it replace the value with a null
  // There is also dateadd or datesub to add or substract days to a date
  val moviesWithReleaseDate = moviesWithDate
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // Diff in days
  moviesWithReleaseDate.show()

  /**
    * Exercise:
    * 1 - How do we deal with changing formats in a date
    * 2 - Read the stocks dataframes and parse the date column
    */
  // 1 - Parse the DF multiple times with all formats that you can identified in the DF and union the small DFS filtering
  // null rows. Alternative is discard malformed dates
  // 2 - Read the stocks
  val stocksDF = spark.read
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true")
    .schema(StructType(Array(
      //symbol,date,price
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    ))).csv("src/main/resources/data/stocks.csv")
  stocksDF.show()

  // STRUCTURES
  // We can create struct structures in a dataframe combining multiple columns like this:
  val structDF = moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))

  // And then select the inner columns like this:
  structDF.select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit")).show()

  // Alternatively, you can use expressions, like
  moviesDF.selectExpr("title", "(US_Gross, Worldwide_Gross) as Profit").selectExpr("Profit.US_Gross as US_Profit")

  // Arrays are more common than structs, examples below
  val moviesWithTitles = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))

  moviesWithTitles.select(col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "love")
  ).show()
}
