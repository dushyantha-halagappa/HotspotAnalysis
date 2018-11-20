package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")

    // TODO : Begin- Keep debug until final debug is done, before submission comment extra debug lines to reduce time
    /*    if (HotcellUtils.DBG) {
          //pickupInfo.show()
          //println(pickupInfo.count())
        }*/
    // TODO : End- Keep debug until final debug is done, before submission comment extra debug lines to reduce time

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))

    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))

    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))

    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    val newCoordinateName = Seq("x", "y", "z")

    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    /** ***************** Begin : Get records ************************/
    // x - Longitude, y - Latitude, z - DayOfMonth
    spark.udf.register("getCount", (latitude: Long, longitude: Long, date: Int) =>
      (HotcellUtils.getCount(latitude, longitude, date, minZ, maxZ, Array(minX, maxX, minY, maxY))))
    spark.udf.register("isValid", (latitude1: Long, longitude1: Long, date1: Int, latitude2: Long, longitude2: Long, date2: Int) =>
      (HotcellUtils.isValid(latitude1, longitude1, date1, latitude2, longitude2, date2, minZ, maxZ, Array(minX, maxX, minY, maxY))))
    spark.udf.register("getPowerOfTwo", (value: Int) => ((HotcellUtils.getPowerOfTwo(value))))

    pickupInfo.createOrReplaceTempView("PickupPointAndDayOfTheMonthView")

    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time
/*    if (HotcellUtils.DBG) {
      pickupInfo.show() // Show pick up points along with day of month info.
      println("Total Available Cells(" + numCells + ")") // Total number of equally divided possible cells.
      println(pickupInfo.count()) // This should be total number of records found in input
    }*/
    // TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time

    val AtLocationTotalDayPickups = spark.sql("SELECT y AS Latitude, x AS Longitude, z AS DayOfMonth, COUNT(*) AS AtLocationTotalDayPickups FROM PickupPointAndDayOfTheMonthView" +
      " WHERE y>= " + minY + " AND y<=" + maxY + " AND x <= " + maxX + " AND x >= " + minX + " AND z <= " + maxZ + " AND z>= " + minZ + " GROUP BY y,x,z ORDER BY y, x,z").persist()
    AtLocationTotalDayPickups.createOrReplaceTempView("AtLocationTotalDayPickupsView")

    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time
/*    if (HotcellUtils.DBG) {
      AtLocationTotalDayPickups.show() // Print total day picks for distinct location.
      println(AtLocationTotalDayPickups.count()) // Total pickups for a given date range
    }*/
    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time

    //where Xj is the attribute value for cell j.
    var temp = spark.sql("SELECT SUM(AtLocationTotalDayPickups) FROM AtLocationTotalDayPickupsView").first()
    val xBarValue: Double = (temp.get(0).toString.toDouble) / numCells

    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time
/*    if (HotcellUtils.DBG) {
      //temp.show()
    }*/
    // TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time

    temp = spark.sql("SELECT SUM(getPowerOfTwo(AtLocationTotalDayPickups)) FROM AtLocationTotalDayPickupsView").first()
    val innerPowerOfTwoTotal = temp.get(0).toString.toDouble
    val sValueTotal: Double = math.sqrt((innerPowerOfTwoTotal / numCells) - HotcellUtils.getPowerOfTwo(xBarValue))

    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time
/*    if (HotcellUtils.DBG) {
      //temp.show()
      println("Sum(XjBar) Calculation->" + xBarValue)
      println("S Value Calculation ->" + sValueTotal)
    }*/
    // TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time

    spark.udf.register("findResult", (nTotal: Int, pickup: Int) => (HotcellUtils.findResult(numCells, xBarValue, sValueTotal, nTotal, pickup)))

    val nearCells = spark.sql("SELECT first.Latitude AS Latitude, first.Longitude AS Longitude, first.DayOfMonth as DayOfMonth, SUM(second.AtLocationTotalDayPickups) AS AtLocationTotalDayPickups, getCount(first.Latitude, first.Longitude, first.DayOfMonth) AS TotalNearCells FROM AtLocationTotalDayPickupsView first cross join AtLocationTotalDayPickupsView second where isValid(first.Latitude, first.Longitude, first.DayOfMonth, second.Latitude, second.Longitude, second.DayOfMonth) group by first.Latitude, first.Longitude, first.DayOfMonth").persist()
    nearCells.createOrReplaceTempView("nearCells")

    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time
/*    if (HotcellUtils.DBG) {
      nearCells.show()
    }*/
    // TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time

    val score = spark.sql("SELECT Latitude, Longitude, DayOfMonth, findResult(TotalNearCells, AtLocationTotalDayPickups) as zScore from nearCells ORDER BY zScore DESC").persist()
    score.createOrReplaceTempView("zScore")
    val result = spark.sql("SELECT Longitude,Latitude, DayOfMonth from zScore")

    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time
/*    if (HotcellUtils.DBG) {
      score.show()
      result.createOrReplaceTempView("result")
      result.show()
    }*/
    // TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time
    /** ******************** Begin : Get records *********************/
    return result
  }
}
