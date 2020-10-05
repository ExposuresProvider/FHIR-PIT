package datatrans

import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.operation.projection.PointOutsideEnvelopeException
import org.opengis.feature.simple.SimpleFeature
import org.opengis.referencing.FactoryException
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.TransformException

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.Point
import org.apache.log4j.{Logger, Level}
import scala.util.control.Breaks._

// Class NearestPoint: Given a file path to a point shapefile (assumed 4326 EPSG) during class initialization, will return the numbers of meters
// from provided point to closest feature in the point shapefile - via the getDistanceToNearestPoint() method.

object NearestPoint {
	private var features: SimpleFeatureCollection = null
}

class NearestPoint(pointShapefilePath : String) {
    
	val log = Logger.getLogger(getClass.getName)
	log.setLevel(Level.INFO)
			
	val shp = new ShapefileHandler(pointShapefilePath)
	val features = shp.getFeatureCollection

	def getDistanceToNearestPoint(lat : Double, lon : Double) : Double = {
		   var destination: Point = null
		   var minDist: Double = Double.PositiveInfinity
		   var distance: Double = 0
		   var minDistPoint: Point = null
		   var lastMatched: SimpleFeature = null
		   
 		   val p = createPoint(lat, lon)
		   
		   val itr = features.features()
		   
		   breakable {
		     while (itr.hasNext()) {
		       val feature = itr.next();
		       val EPSG4326 =
		         "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\","+
		       "SPHEROID[\"WGS 84\",6378137,298.257223563,"+
		       "AUTHORITY[\"EPSG\",\"7030\"]],"+
		       "AUTHORITY[\"EPSG\",\"6326\"]]," +
		       "PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],"+
		       "UNIT[\"degree\", " +"0.01745329251994328,"+
		       "AUTHORITY[\"EPSG\",\"9122\"]],"+
		       "AUTHORITY[\"EPSG\",\"4326\"]]"
		       val crs = CRS.parseWKT(EPSG4326)
		       
		       destination = feature.getDefaultGeometry().asInstanceOf[Point]
		       try {
		         JTS.checkCoordinatesRange(feature.getDefaultGeometry().asInstanceOf[Geometry], crs)
		       }
		       catch {
		         case ex: PointOutsideEnvelopeException => {
		           // ignore any invalid points in the point dataset
		           log.info(s"invalid coordinates found in point dataset")
		           break
		         }
		       }
		       
		       val gc = new GeodeticCalculator(crs)
		       gc.setStartingPosition(
		         JTS.toDirectPosition( p.getCoordinate(), crs))
		       gc.setDestinationPosition(
		         JTS.toDirectPosition(destination.getCoordinate(), crs))
		       distance = gc.getOrthodromicDistance()
		       if (distance < minDist) {
		         minDist = distance
		         minDistPoint = destination
		         lastMatched = feature
		       }
      		     }
      		   }
	  return minDist
	}
	
	def createPoint(lat : Double, lon : Double) : Point = {
	    val geometryFactory = new GeometryFactory()
	    val coordinate = new Coordinate(lon, lat);
	    val point = geometryFactory.createPoint(coordinate)
	    
	    return point
	}

}
