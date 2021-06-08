// Databricks notebook source
// MAGIC %scala
// MAGIC import org.locationtech.jts.geom._
// MAGIC import org.locationtech.geomesa.spark.jts._
// MAGIC 
// MAGIC import org.apache.spark.sql.types._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import spark.implicits._
// MAGIC 
// MAGIC spark.withJTS
// MAGIC import org.locationtech.jts.geom._
// MAGIC import org.locationtech.geomesa.spark.jts._
// MAGIC import org.apache.spark.sql.types._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import spark.implicits._

// COMMAND ----------

// %scala
// import com.uber.h3core.H3Core
// import com.uber.h3core.util.GeoCoord
// import scala.collection.JavaConversions._
// import scala.collection.JavaConverters._

// object H3 extends Serializable {
//   val instance = H3Core.newInstance()
// }

// val geoToH3 = udf{ (latitude: Double, longitude: Double, resolution: Int) => 
//   H3.instance.geoToH3(latitude, longitude, resolution) 
// }
// // val h3ToGeoBoundary = udf{ (h3address: Int,res:Int) => 
// //   H3.instance.h3ToGeoBoundary(h3address) 
// // }
                  
// val polygonToH3 = udf{ (geometry: Geometry, resolution: Int) => 
//   var points: List[GeoCoord] = List()
//   var holes: List[java.util.List[GeoCoord]] = List()
//   if (geometry.getGeometryType == "Polygon") {
//     points = List(
//       geometry
//         .getCoordinates()
//         .toList
//         .map(coord => new GeoCoord(coord.y, coord.x)): _*)
//   }
//   H3.instance.polyfill(points, holes.asJava, resolution).toList 
// }

// val multiPolygonToH3 = udf{ (geometry: Geometry, resolution: Int) => 
//   var points: List[GeoCoord] = List()
//   var holes: List[java.util.List[GeoCoord]] = List()
//   if (geometry.getGeometryType == "MultiPolygon") {
//     val numGeometries = geometry.getNumGeometries()
//     if (numGeometries > 0) {
//       points = List(
//         geometry
//           .getGeometryN(0)
//           .getCoordinates()
//           .toList
//           .map(coord => new GeoCoord(coord.y, coord.x)): _* )
//     }
//     if (numGeometries > 1) {
//       holes = (1 to (numGeometries - 1)).toList.map(n => {
//         List(
//           geometry
//             .getGeometryN(n)
//             .getCoordinates()
//             .toList
//             .map(coord => new GeoCoord(coord.y, coord.x)): _*).asJava 
//       })
//     }
//   }
//   H3.instance.polyfill(points, holes.asJava, resolution).toList 
// }


// COMMAND ----------


import com.uber.h3core.H3Core
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.DataFrame

@transient lazy val h3 = new ThreadLocal[H3Core] {
    override def initialValue() = H3Core.newInstance()
  }


def convertToH3Address(xLong: Double, yLat: Double, precision: Int): String = {
    h3.get.geoToH3Address(yLat, xLong, precision)
  }

val geoToH3Address: UserDefinedFunction = udf(convertToH3Address _)
//h3.get.h3ToGeoBoundary()

def createSpatialIndexAddress(result: String,
                                XLongColumn: String,
                                YLatColumn: String,
                                precision: Int)(df: DataFrame): DataFrame = {
    df.withColumn(
      result,
      geoToH3Address(col(XLongColumn), col(YLatColumn), lit(precision)))
  }


// COMMAND ----------

// %scala

//   def geomUdf = udf { (h3Index: String) =>
//     var hexagonCoords = h3
//       .h3ToGeoBoundary(h3Index)
//       .asScala
//       .map { c => new Coordinate(c.lng, c.lat) }
//       .toArray
//     // Add start point to end of coord array, its not included in
//     // h3 output and JTS linear rings expect it.
//     hexagonCoords = hexagonCoords :+ hexagonCoords(0)
//     val polygon = Polygon(hexagonCoords)
//     polygon.reproject(LatLng, WebMercator)
//   }

// COMMAND ----------

// //h3 -->热区的那个点--->六边形
//   private def h3To6(geoCode:Long): List[GeoCoord] ={
//     val boundary: util.List[GeoCoord] = h3.h3ToGeoBoundary(geoCode)
//     JavaConverters.asScalaIteratorConverter(boundary.iterator()).asScala.toList
//   }

// COMMAND ----------

// %scala
// import com.uber.h3core.H3Core
// val h3 = H3Core.newInstance
// println(h3.h3ToGeoBoundary("8c41766410d17ff"))
// // object h3Test {
// //   val h3 = H3Core.newInstance

// //   def locationToH3(lat: Double, lon: Double, res: Int): Long = {
// //     h3.geoToH3(lat, lon, res)
// //   }

// //   def main(args: Array[String]): Unit = {
// //     val h3code1 = locationToH3(19.715031,110.563673,12)
// // //    val h3code2 = locationToH3(19.715031,110.563673,10)
// //     println("h3code1:"+h3code1);
// // //    println("h3code2:"+h3code2);

// //     println(h3.geoToH3Address(19.7157,110.5636,12))
// //     println(h3.geoToH3Address(19.7149,110.5637,12))
// //     println(h3.geoToH3Address(19.715657,110.563764,12))
// //     println(h3.geoToH3Address(19.715688,110.563662,12))
// //     println(h3.h3ToGeoBoundary("8c41766410d17ff"))
// //     println(h3.h3ToGeoBoundary("8c41766410c29ff"))
// //     println(h3.h3ToGeoBoundary("8c41766410d15ff"))
// //   }
// // }

// COMMAND ----------

// %scala
// import java.util
// import scala.collection.JavaConverters
// import com.uber.h3core.util.GeoCoord

// val h3 = H3Core.newInstance
// val boundary: util.List[GeoCoord]=h3.h3ToGeoBoundary("87536bc05ffffff")
// JavaConverters.asScalaIteratorConverter(boundary.iterator()).asScala.toList


// COMMAND ----------

// %scala
// List<GeoCoord> geoCoords = h3.h3ToGeoBoundary("87536bc05ffffff");

// COMMAND ----------

// val res = 7
// val dfH3 = df.withColumn("h3index", geoToH3(col("pickup_latitude"), col("pickup_longitude"), lit(res)))
// val wktDFH3 = wktDF.withColumn("h3index", multiPolygonToH3(col("the_geom"),lit(res))).withColumn("h3index", explode($"h3index"))

// COMMAND ----------


val df_raw = spark.table("entertainer_b2c_2020.entertainer_geolocation_events")
df_raw.show()

// COMMAND ----------

df_raw.count()

// COMMAND ----------

// from shapely.geometry.polygon import Polygon
// Polygon(h3.h3_to_geo_boundary(h3_hex_string))

// COMMAND ----------

df_raw.transform(createSpatialIndexAddress("h3","lon", "lat", 7)).show(false)

// COMMAND ----------

val df_raw_hex=df_raw.transform(createSpatialIndexAddress("h3","lon", "lat", 7))
df_raw_hex.show()

// COMMAND ----------

// %sql
// drop database geo_hex_mapping;

// COMMAND ----------

// MAGIC %sql
// MAGIC Drop table if exists geo_hexmapping.ent_geoloc_hex_address_2020;

// COMMAND ----------

df_raw_hex.write.format("delta").saveAsTable("geo_hexmapping.ent_geoloc_hex_address_2020")

// COMMAND ----------

// val res = 7
// val dfH3 = df_raw.withColumn("h3index", geoToH3(col("lat"), col("lon"), lit(res)))
// dfH3.show()

// COMMAND ----------



// COMMAND ----------


