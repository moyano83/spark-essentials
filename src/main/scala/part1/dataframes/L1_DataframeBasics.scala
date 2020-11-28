package part1.dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object L1_DataframeBasics extends App{

  // Creating a spark session
  val spark = SparkSession
    .builder()
    .appName("DataframeBasics")
    .master("local[*]")
    .getOrCreate()

  //A dataframe is the description of the schema of the data as well as a distributed collection of rows that forms the data
  // Reading a Dataframe
  val cardsDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/cars.json")

  // Shows data
  cardsDF.show(10)
  // Shows schema
  // Spark types are a collection of case objects that describe the data and are applied at runtime
  cardsDF.schema

  // Schema can also be defined
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // In production inferSchema shouldn't be used, but instead define the schema like above and apply it
  val cardsDFwithSchema = spark.read.schema(carsSchema).format("json").load("src/main/resources/data/cars.json")

  // If you want to create your own data, you can create a Row like this:
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

  // Note that DF has schemas, rows don't
  val manualCarDF = spark.createDataFrame(cars) // schema is autoinferred
  // It is also possible to create DFs with implicits
  import spark.implicits._
  val manualCarsDFwithImplicit = cars.toDF // we can pass also column names

  // Exercises:
  // 1 - Create a manual DF describing smartPhones: maker, model, screen_dimensions, camera_px
  // 2 - Read Another file from data --> movies, and print the schema and count the number of rows

  // Exercise 1:
  val phones = Seq(("Nokia", "3310", "50x50", 3.5), ("Apple", "Iphone 6s", "100x100", 5.0), ("Samsung", "Galaxy", "200x200", 5.5))
  val smartPhonesDF = spark.createDataFrame(phones)
  phones.toDF("Maker", "Model", "Resolution", "Camera Megapixels").show()

  // Exercise 2:
  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"Movies count: ${moviesDF.count()}")
}
