package part2.typesdatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object L4_DataSets extends App{
  val spark = SparkSession.builder().appName("Datasets").master("local[*]").getOrCreate()

  val numbersDF = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("src/main/resources/data/numbers.csv")

  // Spark allows us to add more type information to this dataframe and transform it into a so-called
  // dataset to process this data set with predicates of our choice as if it were a distributed collection of JVM objects
  // which it is. we do this by defining an implicit
  implicit val intEncoders = Encoders.scalaInt
  val datasetOfInt = numbersDF.as[Int]

  // with this in place you can operate on this collection like with any other collection:
  datasetOfInt.filter(_ < 4)

  // What do we do if the object is more complex? Same approach (the case class needs to have the same attributes as the json
  // Step 1: Define your class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .json(s"src/main/resources/data/$filename")

  // All case classes extends the Product type, Encoders.product takes any class that extends Product, so we can easily
  // define the implicit as follows:
  // implicit val carEncoder = Encoders.product[Car]
  // Step 2: Read the DF
  val carsDF = readDF("cars.json")
  import spark.implicits._


  // Step 3: define the encoder
  // and then we can do:
  // Step 4: convert the DF to DS
  val carsDS = carsDF.as[Car]
  // There is another way to simplify the above, we can just import spark.implicits, which imports all the Encoders which
  // we might need, so we can remove the implicit declaration by just importing spark.implicits._

  // With the dataset, we can operate like with any other collection
  val otherCarDS = carsDS.map(car => car.copy(Name=car.Name.toUpperCase()))

  // We can also show the collection
  otherCarDS.show(10)

  // DataSets are useful when:
  // 1 - We want to maintain Type information
  // 2 - We want clear and concise code
  // 3 - our filters and transformations are hard to express in DF or SQL
  // Avoid DS when:
  // 1 - Performance is critical as spark can't optimize transformations

  /**
    * Exercises:
    */
  // 1 - Count how many cars there is
  val carsCount = carsDS.count()
  println(carsCount)
  // 2 - Count how many powerful cars there are (horsepower > 140)
  println(carsDS.filter(car=> car.Horsepower.getOrElse(0L) > 140).count())
  // 3 - Compute the average horsepower for the DS
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // 3 - BIS
  // All datasets have access to the DF functions, this is because DataFrame is a type alias for Dataset[Row]
  carsDS.select(avg(col("HorsePower"))).show()

  case class Guitar(id:Long, model:String, make:String, `type`: String)
  case class GuitarPlayer(id:Long, name:String, guitars:Seq[Long], band: Long)
  case class Band(id:Long, name:String, hometown:String, year:Long)

  val guitars = readDF("guitars.json").as[Guitar]
  val guitarPlayers = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bands = readDF("bands.json").as[Band]

  // Part 2:
  // JOINS with Datasets
  // joinWith will keep the type information by returning a Tuple of both types
  val guitBands = guitarPlayers.joinWith(bands, guitarPlayers.col("band") === bands.col("id"), "inner")
  guitBands.show()

  /**
    * Exercise: Join the GuitarPlayers with Guitars with the condition that the guitar id is contained in the guitar
    * players guitars
    */
  guitarPlayers.joinWith(guitars, array_contains(guitarPlayers.col("guitars"), guitars.col("id")), "outer").show()

  // Grouping DS: By Key
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin) // Returns a KeyValuedGroupedDataset[String, Car]
  carsGroupedByOrigin.count().show() // Contains the count per key
  // Bear in mind that joins and groups are wide transformations because it can change the number of partitions in the DS,
  // involves shuffles
}
