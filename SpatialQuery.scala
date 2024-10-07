package cse511
//Imports 'sparksession' class from Apache Spark
import org.apache.spark.sql.SparkSession

object SpatialQuery extends App {

  // A function that checks if a point is trapped in a rectangular cage.
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    // Parse the pointString and queryRectangle
    val point = pointString.split(",").map(_.toDouble)
    val rectangle = queryRectangle.split(",").map(_.toDouble)

    // Point coordinates
    val pointY = point(1)
    val pointX = point(0)
    

    // Rectangle coordinates (minX, minY, maxX, maxY)
    
    val rectMaxY = rectangle(3)
    val rectMaxX = rectangle(2)
    val rectMinY = rectangle(1)
    val rectMinX = rectangle(0)  
    

    // Test for point-rectangle intersection, including boundary points.
    (pointX >= rectMinX && pointX <= rectMaxX) && (pointY >= rectMinY && pointY <= rectMaxY)
  }

  // User-defined function that measures the distance between two points.
  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    // Parse the pointString1 and pointString2
    val point1 = pointString1.split(",").map(_.toDouble)
    val point2 = pointString2.split(",").map(_.toDouble)

    // Point1 coordinates
    val point1X = point1(0)
    val point1Y = point1(1)

    // Point2 coordinates
    val point2X = point2(0)
    val point2Y = point2(1)

    // Calculate the Euclidean distance between point 1 and point 2
    val euclideanDistance = math.sqrt(math.pow(point2X - point1X, 2) + math.pow(point2Y - point1Y, 2))

    // Check if the distance between the points is less than or equal to the given distance
    euclideanDistance <= distance
  }

  // Run the Range Query (uses ST_Contains)
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf.createOrReplaceTempView("point")

    // Register the ST_Contains User defined function
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => ST_Contains(queryRectangle, pointString))

    // Execute the query using the User defined funtion
    val resultDf = spark.sql(s"SELECT * FROM point WHERE ST_Contains('$arg2', point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  // Run the Range Join Query (uses ST_Contains)
  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2)
    rectangleDf.createOrReplaceTempView("rectangle")

    // Register the ST_Contains User defined functions
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => ST_Contains(queryRectangle, pointString))

    // Execute the join query using the User defined functions
    val resultDf = spark.sql("SELECT * FROM rectangle, point WHERE ST_Contains(rectangle._c0, point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  // Run the Distance Query (uses ST_Within)
  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf.createOrReplaceTempView("point")

    // Register the ST_Within User defined functions
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => ST_Within(pointString1, pointString2, distance))

    // Execute the query using the User defined functions
    val resultDf = spark.sql(s"SELECT * FROM point WHERE ST_Within(point._c0, '$arg2', $arg3)")
    resultDf.show()

    return resultDf.count()
  }

  // Run the Distance Join Query (uses ST_Within)
  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {
    val pointDf1 = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf1.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2)
    pointDf2.createOrReplaceTempView("point2")

    // Register the ST_Within User defined functions
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => ST_Within(pointString1, pointString2, distance))

    // Invoke the join query employing user-defined procedures
    val resultDf = spark.sql(s"SELECT * FROM point1 p1, point2 p2 WHERE ST_Within(p1._c0, p2._c0, $arg3)")
    resultDf.show()

    return resultDf.count()
  }
}
