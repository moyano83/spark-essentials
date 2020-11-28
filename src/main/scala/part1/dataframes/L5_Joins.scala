package part1.dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import part1.dataframes.L2_Datasources.spark

object L5_Joins extends App{
  // Creating a spark session
  val spark = SparkSession
    .builder()
    .appName("Joins")
    .master("local[*]")
    .getOrCreate()

  val guitarsDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/guitars.json")
  val guitarPlayersDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/bands.json")

  val guitarsAndBands = guitarPlayersDF.join(bandsDF, guitarPlayersDF("band") === bandsDF("id"), "inner")
  guitarsAndBands.show()

  // Join types
  //left_outer
  val guitarsAndBandsLeftOuter = guitarPlayersDF.join(bandsDF, guitarPlayersDF("band") === bandsDF("id"), "left_outer")
  guitarsAndBandsLeftOuter.show()

  // Right_outer
  val guitarsAndBandsRightOuter = guitarPlayersDF.join(bandsDF, guitarPlayersDF("band") === bandsDF("id"), "right_outer")
  guitarsAndBandsRightOuter.show()

  // Outer
  val guitarsAndBandsOuter = guitarPlayersDF.join(bandsDF, guitarPlayersDF("band") === bandsDF("id"), "outer")
  guitarsAndBandsOuter.show()

  // SEMI-JOINS
  // Left_semi : Like an inner join but we just select the left dataframe that satisfies the condition
  val guitarsAndBandsLeftSemi = guitarPlayersDF.join(bandsDF, guitarPlayersDF("band") === bandsDF("id"), "left_semi")
  guitarsAndBandsLeftSemi.show()

  // Left_anti : Like an inner join but we just select the left dataframe that does not satisfies the condition
  val guitarsAndBandsLeftAnti = guitarPlayersDF.join(bandsDF, guitarPlayersDF("band") === bandsDF("id"), "left_anti")
  guitarsAndBandsLeftAnti.show()

  // If we join two DF with the same column names, spark will crash, we have the following options:
  // 1 - Rename one of the columns
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
  // 2 - Drop the dupe column
  // this would take guitarsAndBands and look for the column coming from bandsDF, which spark is able to resolve because
  // all the columns has an unique identifier
  guitarsAndBands.drop(bandsDF.col("id"))

  // 3 - Rename the offending column
  val renamedBandDF = bandsDF.withColumnRenamed("id", "the_band")
  guitarPlayersDF.join(renamedBandDF, guitarPlayersDF("band") === renamedBandDF("the_band"), "inner")

  // Selecting the guitarrists and their Guitars
  guitarPlayersDF.withColumn("Guitar", explode(col("guitars"))).drop("guitars").as("guitarrists")
    .join(guitarsDF.as("guitarBrands"), expr("guitarrists.guitar=guitarBrands.id")).show()

  // Another way to do the above
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show()

  /**
    * Exercises:
    * 1 - Show all public.employees and their max salary from salaries table (emp_no in both tables)
    * 2 - Show all employees who were never managers: left_outer with table dept_manager
    * 3 - Find the job titles of the best paid 10 employees on the company: titles table (emp_no)
    */
  def getTableFromDB(tableName:String): DataFrame =spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/test_spark")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.${tableName}")
    .load()

  val employeesDF = getTableFromDB("employees").withColumnRenamed("emp_no", "id")
  val salariesDF = getTableFromDB("salaries")
  val deptManagerDF = getTableFromDB("dept_manager")
  val titlesDF = getTableFromDB("titles")

  // 1
  employeesDF.join(salariesDF, employeesDF("id")  === salariesDF("emp_no"))
    .groupBy("id", "first_name", "last_name").agg(max("salary").as("Max_Salary")).show()
  // 2
  employeesDF.join(deptManagerDF, employeesDF("id") === deptManagerDF("emp_no"), "left_anti").show()
  // 3
  employeesDF.join(titlesDF, employeesDF("id") === titlesDF("emp_no"), "inner")
    .groupBy("id", "title").agg(max(titlesDF("to_date")))
    .join(salariesDF, employeesDF("id")  === salariesDF("emp_no"))
    .groupBy(employeesDF("id"), titlesDF("title")).agg(max(salariesDF("salary")).as("Money"))
    .orderBy(col("Money").desc)
    .show(10)
}
