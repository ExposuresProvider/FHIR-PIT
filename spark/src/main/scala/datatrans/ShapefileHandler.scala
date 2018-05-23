package datatrans

import java.io.File
import java.io.IOException
import java.net.MalformedURLException
import java.net.URL
import java.util.HashMap

import org.geotools.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.SchemaException
import org.opengis.referencing.FactoryException
import org.opengis.referencing.operation.TransformException

class ShapefileHandler(shpFilePath : String) {

  @throws(classOf[SchemaException])
  @throws(classOf[FactoryException])
  @throws(classOf[TransformException])
  @throws(classOf[IOException])
  @throws(classOf[MalformedURLException])
  def getFeatureCollection() : SimpleFeatureCollection = {

    val file = new File(this.shpFilePath)
    val map = new HashMap[String, URL]()
    map.put("url", file.toURI().toURL())

    val dataStore= DataStoreFinder.getDataStore(map)
    val featureSource = dataStore.getFeatureSource(dataStore.getTypeNames()(0))
    featureSource.getFeatures()

  }

}
