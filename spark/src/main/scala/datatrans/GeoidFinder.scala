package datatrans

import org.geotools.data.collection.SpatialIndexFeatureCollection
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.feature.simple.SimpleFeature
import org.opengis.referencing.FactoryException
import org.opengis.referencing.operation.TransformException

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.Geometry

object GeoidFinder {
  private val gf = new GeometryFactory()
  private var index = new SpatialIndexFeatureCollection()
}

class GeoidFinder(blockgrpShapefilePath : String, geoidPrefix: String = "")  {

  val shp = new ShapefileHandler(blockgrpShapefilePath)
  val features = shp.getFeatureCollection
  GeoidFinder.index  = new SpatialIndexFeatureCollection(features.getSchema)
  GeoidFinder.index.addAll(features)


  private def getCensusBlockContainingPoint(p : Point): SimpleFeature = {

    var feature : SimpleFeature = null


    val itr = GeoidFinder.index.features()
    var done = false
    while (itr.hasNext && !done) {
      feature = itr.next

      def geom = feature.getDefaultGeometry.asInstanceOf[Geometry]

      if (geom.contains(p)) {
        done = true
      }
    }
    if(done) {

      feature
    } else {
      null
    }



  }

  @throws(classOf[TransformException])
  @throws(classOf[FactoryException])
  private def createPointLCC(lat : Double, lon : Double) : Point = {

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

    GeoidFinder.gf.createPoint(targetCoordinate)

  }

  def getGeoidForLatLon(lat : Double, lon : Double) : Option[String] = {

    var p : Point = null

    try {
      p = createPointLCC(lat, lon)
      val feature = getCensusBlockContainingPoint(p)
      val id = feature.getAttribute("GEOID").asInstanceOf[String]
      Some(geoidPrefix + id)
    }
    catch {
      case e : Exception  =>
        System.out.println(e)
        None
    }


  }

}
