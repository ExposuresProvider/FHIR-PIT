package datatrans

import org.geotools.data.collection.SpatialIndexFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTS
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.opengis.feature.simple.SimpleFeature
import org.opengis.geometry.BoundingBox
import org.opengis.referencing.FactoryException
import org.opengis.referencing.operation.TransformException

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.linearref.LocationIndexedLine
import com.vividsolutions.jts.geom.Point
import org.apache.log4j.{Logger, Level}

object NearestRoad {
  private val ff = CommonFactoryFinder.getFilterFactory2()
  private val gf = new GeometryFactory()
}

class NearestRoad(roadShapefilePath : String, maximum_search_radius : Double) {

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  val shp = new ShapefileHandler(roadShapefilePath)
  val features = shp.getFeatureCollection
  val index : SpatialIndexFeatureCollection = new SpatialIndexFeatureCollection(features.getSchema)
  var lastMatched : Option[SimpleFeature] = None
  index.addAll(features)

  def getMinimumDistance(lat : Double, lon : Double) : Double = {

    val p = createPointLCC(lat, lon)
    findMinimumDistance(p)

  }

  private def findMinimumDistance(p : Point) : Double = {

    val coordinate = p.getCoordinate
    val search = new ReferencedEnvelope(new Envelope(coordinate),
      index.getSchema.getCoordinateReferenceSystem)
    search.expandBy(maximum_search_radius)
    val bbox = NearestRoad.ff.bbox(NearestRoad.ff.property(index.getSchema.getGeometryDescriptor.getName),
      search.asInstanceOf[BoundingBox])

    val candidates = index.subCollection(bbox)

    var minDist: Double = -1
    val itr = candidates.features()

    while (itr.hasNext) {
      val feature = itr.next()

      // use following 2 lines to get road name
      //attribute = feature.getAttribute("FULLNAME").asInstanceOf[String]
      //System.out.println(attribute)

      // log.info(s"feature = $feature")
      val line = new LocationIndexedLine(feature.getDefaultGeometry.asInstanceOf[Geometry])
      val here = line.project(coordinate)
      val point = line.extractPoint(here)
      val dist = point.distance(coordinate)
      if (dist <= maximum_search_radius && (minDist < 0 || dist < minDist)) {
        minDist = dist
        lastMatched = Some(feature)
      }
    }

    log.debug(s"p = $p, minDist = $minDist")
    minDist
  }

  def getLastMatched : Option[SimpleFeature] = lastMatched

  @throws(classOf[TransformException])
  @throws(classOf[FactoryException])
  private def createPointLCC(lat : Double, lon : Double) : Point =  {

    val wkt = "PROJCS[\"AQMEII_CMAQ\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\"," +
      "SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0]," +
      "UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Conformal_Conic\"]," +
      "PARAMETER[\"False_Easting\",-2556000.0],PARAMETER[\"False_Northing\",-1728000.0]," +
      "PARAMETER[\"Central_Meridian\",-97.0],PARAMETER[\"Standard_Parallel_1\",33.0]," +
      "PARAMETER[\"Standard_Parallel_2\",45.0],PARAMETER[\"Scale_Factor\",1.0]," +
      "PARAMETER[\"Latitude_Of_Origin\",40.0],UNIT[\"Meter\",1.0]]"

    val crs = CRS.parseWKT(wkt)
    val sourceCRS = CRS.decode("EPSG:4326")

    val c = new Coordinate(lat, lon)
    val transform = CRS.findMathTransform(sourceCRS, crs)
    val targetCoordinate = JTS.transform(c, null, transform )

    NearestRoad.gf.createPoint(targetCoordinate)

  }

  def getMatchedRoadName : Option[String] = lastMatched.map(_.getAttribute("FULLNAME").asInstanceOf[String])

  def getMatchedRouteId : Option[String] = lastMatched.map(_.getAttribute("ROUTE_ID").asInstanceOf[String])
  
  def getMatchedNumLanes : Option[String] = lastMatched.map(_.getAttribute("THROUGH_LA").toString)
  
  def getMatchedSpeed : Option[String] = lastMatched.map(_.getAttribute("SPEED").toString)
  
  def getMatchedRoadType : Option[String] = lastMatched.map(_.getAttribute("ROADTYPE").asInstanceOf[String])

}
