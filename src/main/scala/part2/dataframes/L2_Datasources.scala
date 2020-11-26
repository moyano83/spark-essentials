package part2.dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2.dataframes.L1_DataframeBasics.{carsSchema, spark}

object L2_Datasources extends App{
  // Creating a spark session
  val spark = SparkSession
    .builder()
    .appName("DataSources")
    .master("local[*]")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  // Reading a DF would need the following:
  // 1 - A Format
  // 2 - A Schema (Optional)
  // 3 - 0 or more options
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // Defines what spark do in case it encounter a malformed schema: dropMalformed,permissive(default)
    // .option("path", "src/main/resources/data/cars.json") <-- Another way to provide the data
    // .options(Map(.....)) <-- this is a way to pass all the options at the same time
    .load("src/main/resources/data/cars.json") // A local or remote file

  // Writting DFs requires:
  // 1 - Format
  // 2 - Save mode => override, append, ignore, errorIfExists
  // 3 - path
  // 4- Zero or more options
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/CarsDuplicate") // This is a folder, not the final file
    .save() // Or you can provide the path as an argument of this method

  // JSON Flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") //For this to work you must specify a schema and the field has to be DateType
    .option("allowSingleQuotes", "true") // For Json that is formatted with single quotes
    .option("compression", "uncompressed") //bzip2, gzip, lz4, snappy, deflate (any of this format is natively supported)
    .json("src/main/resources/data/cars.json")

  // CSV Flags
  spark.read
    .schema(StructType(
              Array(StructField("symbol", StringType), StructField("date", DateType), StructField("price", DoubleType))))
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // This ignores the first row, and validates the first row against your schema
    .option("sep", ",") // Indicate the separator
    .option("nullValue", "") // Specifies the null value
    .csv("src/main/resources/data/stocks.csv")

  // PARQUET FLAGS (Default storage formats for DFs
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/parquet") // Applies default snappy compression by default

  // Text Files Flags
  // If you read a text file every single line on the text file is considered a value in a single column data frame
  spark.read.text("src/main/resources/data/stocks.csv").show()

  // Reading from a remote DB (run the posgress DB)
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/test_spark")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show(20)

  // Exercise, read the movies DF, write it as a tab separated values file, snappy parquet and as a table in the postgres
  // DB named public.movies

  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")

  moviesDF.write
    .option("sep", "\t") // Indicate the separator
    .option("header", "true")
    .csv("src/main/resources/data/movies_csv")

  moviesDF.write
    .option("compression", "snappy")
    .parquet("src/main/resources/data/movies_parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/test_spark")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies_test")
    .save()
}
