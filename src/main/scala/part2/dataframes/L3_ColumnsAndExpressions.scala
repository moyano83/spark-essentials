package part2.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import part2.dataframes.L2_Datasources.spark
object L3_ColumnsAndExpressions extends App{
  // Creating a spark session
  val spark = SparkSession
    .builder()
    .appName("Columns And Expressions")
    .master("local[*]")
    .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .load("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name") // This is a JVM object with no data on it

  // Selecting columns (called projection)
  val newCarsDF = carsDF.select(firstColumn) // you obtain a new DF

  //There is several ways to select columns in a spark DF
  // 1- Pass the columnn from the DF
  carsDF.select(carsDF.col("Name"), carsDF.col("Acceleration"))
  // 2- Use the col or column method from spark sql functions
  carsDF.select(col("Name"), col("Acceleration"))
  // 3 - Use symbols from the spark implicits
  import spark.implicits._
  carsDF.select('Year) //The symbol 'Year is transformed to a column through implicits
  // 4 - Use the $ and double quotes like in s interpolated strings
  carsDF.select($"Year")
  // 5 - Use expr
  carsDF.select(expr("Origin")) // This is converted to an Expression that returns the Origin columns
  // 6 - Pass a collection of strings
  carsDF.select("Name", "Year")

  // More in Expressions on Spark
  val weightInLbs = carsDF.col("Weight_in_lbs") // Column is a subtype of an expression
  val weightInKg = carsDF.col("Weight_in_lbs") / 2.2 // The result of this is also an expression

  val anotherWeightColumnInKG = expr("Weight_in_lbs / 2.2").as("Expr_Weight_in_Kg")

  val carsAndWeight = carsDF.select(col("Name"), col("Acceleration"), weightInKg.as("Weight_in_Kg"))

  // If you have loads of expr cols, you can use the selectExpr
  carsDF.selectExpr("Name", "Weight_in_lbs / 2.2") // This is a short-hand for selecting expr without calling the expr

  // DF Processing
  // Adding a column
  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // Renaming a column, column names have restricted characters so you need `
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "`Weight in pounds`")
  // Remove columns
  val otherCars = carsWithColumnRenamed.drop("`Weight in pounds`")
  //filtering
  val europeanCarsDF = carsWithColumnRenamed.filter(col("Origin") =!="USA")
  // Another way to do it
  val europeanCarsDF2 = carsWithColumnRenamed.where(col("Origin") =!= "USA")
  // Same with expressions
  val europeanCarsDF3 = carsWithColumnRenamed.filter("Origin != 'USA'")
  // Chaining Filters
  val americanPowerfulCarsDF1 = carsWithColumnRenamed.filter("Origin != 'USA'").filter(col("Horsepower") > 150)
  // Same with And operators
  val americanPowerfulCarsDF2 = carsWithColumnRenamed.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  // With expressions
  val americanPowerfulCarsDF3 = carsWithColumnRenamed.filter("Origin != 'USA' and Horsepower > 150")

  //Imagine that we want to add more rows to our DF
  val moreCarsDF = spark.read.format("json").option("inferSchema", "true").load("src/main/resources/data/more_cars.json")
  val allCars = carsDF.union(moreCarsDF)

  // We can also find the number of distinct values on a column or DF
  val countries = carsDF.select("Origin").distinct()
  countries.show()

  // Exercises: Read the movies DF and do the following:
  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")
  // 1 - Select two columns of your choice
  moviesDF.select('Source, $"Title").show(10)
  // 2 - Sum up all the gross profits for all the movies, name the column  "total_profit"
  moviesDF.select((col("US_Gross") +
    col("Worldwide_Gross") +
    when(col("US_DVD_Sales").isNull, 0).otherwise(col("US_DVD_Sales"))).as("total_profit")).show(10)
  // 3 - Select All the comedy movies with IMDB rating above 6
  moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").show(10)
  // Or combining wheres
  moviesDF.where(col("Major_Genre") === "Comedy").where(col("IMDB_Rating") > 6).show(10)


}
