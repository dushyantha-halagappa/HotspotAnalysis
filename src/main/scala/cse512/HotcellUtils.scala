package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01
  val DBG = false

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def getPowerOfTwo(value: Double): Double = {
    return Math.pow(value, 2)
  }

  // http://sigspatial2016.sigspatial.org/giscup2016/problem

  def getCount(latitude: Long, longitude: Long, date: Int, minDateRange: Int, maxDateRange: Int, boundary: Array[Double]): Int = {
    var total = 0
    for (i <- longitude - 1 to longitude + 1) {
      for (j <- latitude - 1 to latitude + 1) {
        for (k <- date - 1 to date + 1) {
          if (i >= boundary(0) && i <= boundary(1)
            && j >= boundary(2) && j <= boundary(3)
            && k >= minDateRange && k <= maxDateRange) {
            total += 1
            //println(total)
          }
        }
      }
    }
    return total
  }

  def isValid(latitude1: Long, longitude1: Long, date1: Int, latitude2: Long, longitude2: Long, date2: Int, minDateRange: Int, maxDateRange: Int, boundary: Array[Double]): Boolean = {
    if (latitude1 - 1 == latitude2 || latitude1 == latitude2 || latitude1 + 1 == latitude2) {
      if (longitude1 - 1 == longitude2 || longitude1 == longitude2 || longitude1 + 1 == longitude2) {
        if (date1 - 1 == date2 || date1 == date2 || date1 + 1 == date2) {
          return true
        }
      }
    } else if (latitude2 < boundary(0) || latitude2 > boundary(1) || longitude2 < boundary(2) || longitude2 > boundary(3) || date2 > maxDateRange || date2 < minDateRange) {
      return false
    }

    return false
  }


  def getRange(bound: Double, cells: Int): Double = {
    return bound / cells
  }

  def findResult(numOfCell: Double, XBarValue: Double, SValueTotal: Double, neighbours: Int, grandSum: Int): Double = {
    // TODO : Begin-Keep this line only to debug, before submission comment this line to reduce time

    if (DBG) {
      println("findResult")
      println(numOfCell)
      println(XBarValue)
      println(SValueTotal)
      println(neighbours)
      println(grandSum)
    }

    // TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time
    //return cells//(latitude.toDouble - getRange(total, cells) * longitude) / (math.sqrt(((cells.toDouble * longitude) - (longitude.toDouble * longitude)) / (cells.toDouble - 1)) * (math.sqrt((grandTotal / cells) - (getRange(total, cells) * getRange(total, cells)))))


    val numeratorFinding = (grandSum.toDouble - (XBarValue * neighbours.toDouble))
    val denominatorFinding = SValueTotal * math.sqrt((((numOfCell.toDouble * neighbours.toDouble) - (neighbours.toDouble * neighbours.toDouble)) / (numOfCell.toDouble - 1.0)))

    // TODO : Begin-Keep debug until final debug is done, before submission comment extra debug lines to reduce time
    if (DBG) {
      println(numeratorFinding)
      println(denominatorFinding)
    }
    // TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time

    return (numeratorFinding / denominatorFinding)
  }

  def ST_Contains(x: Int, y: Int, minX: Int, minY: Int, maxX: Int, maxY: Int): Boolean = {
    if (minX <= x && minY <= y && maxX >= x && maxY >= y) {
      true
    } else {
      false
    }
  }
}