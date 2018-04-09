package datatrans;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.SchemaException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;


public class ShapefileHandler {
	
	private String shpFilePath;
	public ShapefileHandler(String shpFilePath) {
		this.shpFilePath = shpFilePath;
	}
	
	public SimpleFeatureCollection getFeatureCollection() throws SchemaException, FactoryException, TransformException, IOException, MalformedURLException {
		File file = new File(this.shpFilePath);
		Map<String, URL> map = new HashMap<String, URL>();      
		map.put("url", file.toURI().toURL());
	
		DataStore dataStore = DataStoreFinder.getDataStore(map);
	
		SimpleFeatureSource featureSource = dataStore.getFeatureSource(dataStore.getTypeNames()[0]);        
		SimpleFeatureCollection collection = featureSource.getFeatures();
		
		return collection;
	}
	
	
	public void getDistancetoClosestRoad(double lat,  double lon) {
		
		
	}

}
