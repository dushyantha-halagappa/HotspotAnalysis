package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val pointCoordinates = pointString.split(",").map(_.toDouble)
    val rectangleCoordinates = queryRectangle.split(",").map(_.toDouble)
    val pickupPointXAxis = pointCoordinates(0)
    val pickupPointYAxis = pointCoordinates(1)
    val boundLatitude1 = rectangleCoordinates(0)
    val boundLongitude1 = rectangleCoordinates(1)
    val boundLatitude2 = rectangleCoordinates(2)
    val boundLongitude2 = rectangleCoordinates(3)

    /** TODO : Keep this line only to debug, before submission comment this line to reduce time **/
    //printf("Point Coordinates->(%s : %s)", pickupPointXAxis, pickupPointYAxis)
    //printf("Rectangle Coordinates->(%s : %s : %s : %s)", boundLatitude1, boundLongitude1, boundLatitude2, boundLongitude2)
    //println()
    //printf("(%s : %s : %s : %s)",
    //  math.max(boundLatitude1, boundLatitude2),
    //  math.min(boundLatitude1, boundLatitude2),
    //  math.max(boundLongitude1, boundLongitude2),
    //  math.min(boundLongitude1, boundLongitude2))
    /** TODO : End-Keep debug until final debug is done, before submission comment extra debug lines to reduce time **/

    if (pickupPointXAxis <= math.max(boundLatitude1, boundLatitude2)
      && pickupPointXAxis >= math.min(boundLatitude1, boundLatitude2)
      && pickupPointYAxis <= math.max(boundLongitude1, boundLongitude2)
      && pickupPointYAxis >= math.min(boundLongitude1, boundLongitude2)) {
      // TODO : Comment print statements before submission
      //print("true")
      return true
    }

    // TODO : Comment print statements before submission
    //print("false")

    return false
  }
}
