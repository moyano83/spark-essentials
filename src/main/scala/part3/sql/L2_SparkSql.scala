package part3.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object L2_SparkSql extends App{

  val spark = SparkSession.builder().appName("Spark SQL practice")
    // This config is required because after spark 2.4 the SaveMode.Override behaviour has changed, and you need this to
    // overwrite the data when you attempt to rerun an Apache Spark write operation by cancelling the currently running
    // job, the problem is that a metadata directory called _STARTED isn’t deleted automatically. This flag deletes the
    // _STARTED directory and returns the process to the original state. (deprecated, removed on 3.0.0)
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .master("local[*]")
    .getOrCreate()

  // How do you transfer tables from database to spark sql?
  def getTableFromDB(tableName:String): DataFrame =spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/test_spark")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.${tableName}")
    .load()

  def saveTable(df:DataFrame, tableName:String) = df.write.mode(SaveMode.Overwrite).saveAsTable(tableName.capitalize)

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  // Example of regular API usage
  carsDF.select(col("Name")).where(col("Origin") === "USA").show()

  // We can use spark sql to do the same (this create an alias in spark so you can refer to it as a table)
  carsDF.createOrReplaceTempView("cars")
  // Then you can do
  spark.sql(
    """
      |select Name from cars where Origin="USA"
      |""".stripMargin).show() // We can do this because sql returns a DF

  // You can create databases, which will trigger spark to create a spark-warehouse in the root of the project, we can
  // change the path to this by defining the option spark.sql.warehouse.dir
  spark.sql("create database test_spark")

  // The sql api is very flexible
  spark.sql("use test_spark")
  spark.sql("show databases").show()

  // How do we transfer data from a db to spark db? (start the docker postgress first
  def transferData(tableNames:List[String]) = tableNames.foreach(name =>saveTable(getTableFromDB(name), name))

  transferData(List("departments", "dept_emp", "dept_manager", "employees", "salaries", "titles "))

  // Now we can read the data from the warehouse like:
  //val employees = spark.read.table("Employees")

  /**
    * Exercises:
    * 1 - Read the movies DF and store it as a spark table in the test_spark db
    */
  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")
  moviesDF.write.mode(SaveMode.Overwrite).saveAsTable("movies")
  /**
    * 2 - Count how many employees were hired between 1-1-2000 and 1-1-2001
    */
  spark.sql(
    """
      |select count(*) from Employees where hire_date between '2000-01-01' and '2001-01-01'
      |""".stripMargin).show()
  /**
    * 3 - Show the avg salaries for the employees hired in (2) group by department number
    */
  spark.sql(
    """
      |select avg(s.salary), d.dept_no from salaries s inner join employees e on s.emp_no = e.emp_no inner join dept_emp d on
      | d.emp_no = e.emp_no where e.hire_date between '2000-01-01' and '2001-01-01' group by d.dept_no
      |""".stripMargin).show()
  /**
    * 4 - Show the name of the best paying department for employees hired in (2)
    */
  spark.sql(
    """
      |select * from (select avg(s.salary) as retribution, d.dept_name from salaries s
      | inner join employees e on s.emp_no = e.emp_no
      | inner join dept_emp dt on dt.emp_no = e.emp_no
      | inner join departments d on d.dept_no = dt.dept_no
      | where e.hire_date between '2000-01-01' and '2001-01-01'
      | group by d.dept_name) as t sort by t.retribution desc limit 1
      |""".stripMargin).show()
}
