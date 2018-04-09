package datatrans;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.SchemaException;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.distance.DistanceOp;

public class TestDistance {
	
	private String shpFilePath;
	private String wkt = "PROJCS[\"AQMEII_CMAQ\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Conformal_Conic\"],PARAMETER[\"False_Easting\",-2556000.0],PARAMETER[\"False_Northing\",-1728000.0],PARAMETER[\"Central_Meridian\",-97.0],PARAMETER[\"Standard_Parallel_1\",33.0],PARAMETER[\"Standard_Parallel_2\",45.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",40.0],UNIT[\"Meter\",1.0]]";
	private String proj = "+proj=lcc +lon_0=-97 +lat_0=40.0 +lat_1=33.0 +lat_2=45.0 +units=meters";
	
	public TestDistance(String shpFilePath) {
		this.shpFilePath = shpFilePath;
	}
	
	public SimpleFeatureCollection getFeatureCollection() throws SchemaException, FactoryException, TransformException, IOException, MalformedURLException {
		File file = new File(this.shpFilePath);
		Map<String, URL> map = new HashMap<String, URL>();      
		map.put("url", file.toURI().toURL());
	
		DataStore dataStore = DataStoreFinder.getDataStore(map);
	
		// SimpleFeatureSource featureSource = dataStore.getFeatureSource(shpName); this works too
		SimpleFeatureSource featureSource = dataStore.getFeatureSource(dataStore.getTypeNames()[0]);        
		SimpleFeatureCollection collection = featureSource.getFeatures();
		
		GeometryFactory gf = JTSFactoryFinder.getGeometryFactory();
		//CoordinateReferenceSystem crs = CRS.parseWKT(wkt);
		CoordinateReferenceSystem crs = CRS.decode("EPSG:4326");
		CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:4326");
		
		Coordinate c = new Coordinate(-80.482165, 36.352873);
	    //MathTransform transform = CRS.findMathTransform(sourceCRS, crs);
	    //Coordinate targetCoordinate = JTS.transform(c, null, transform );
	    Point p = gf.createPoint(c);
	    //final SimpleFeatureType TYPE = DataUtilities.createType("Location", "the_geom:Point:srid=4326,");
	    //SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);
	    //featureBuilder.add(p);
        //SimpleFeature p_feature = featureBuilder.buildFeature(null);
        //Geometry tmp_geom = (Geometry) p_feature.getDefaultGeometry();
        //Geometry p_geom = JTS.transform(tmp_geom, transform );
        //System.out.println(p_geom);
        
//		SimpleFeatureType schema = featureSource.getSchema();
//		String geomType = schema.getGeometryDescriptor().getType().getBinding().getName();
//		System.out.println(geomType);
	    double[] distances = new double[collection.size()];
		SimpleFeatureIterator iterator = collection.features();
		int index = 0;
	    try {
	        while( iterator.hasNext() ){
	            SimpleFeature feature = iterator.next();
	            Geometry geom = (Geometry) feature.getDefaultGeometry();
	            Coordinate[] coords = geom.getCoordinates();
	            //for (int i = 0; i < coords.length; i++) {
	            //distances[index++] = DistanceOp.distance(p_geom, geom);
	    		GeodeticCalculator gc = new GeodeticCalculator();
	    		gc.setStartingPosition( JTS.toDirectPosition(  DistanceOp.closestPoints(geom, p)[0], crs ) );
	    		//gc.setDestinationPosition( JTS.toDirectPosition( p, crs ) );
	    //
//	    		double distance = gc.getOrthodromicDistance();
	        }
	    }
	    finally {
	        iterator.close();
	    }
	    Arrays.sort(distances);
	    System.out.println(distances[0]);
	    System.out.println(distances[307803]);
	    //System.out.println(Arrays.toString(distances));
		
		// just get one feature to get the geom info
//		SimpleFeature feature = collection.features().next();
//		Geometry geo = (Geometry) feature.getDefaultGeometry();
//		
//	    Coordinate[] pts = DistanceOp.distance(p, collection);
	  
	    
//	    double distance = geo.distance(p);
//	    System.out.println(distance);
	    
	    
//	    CoordinateReferenceSystem crs = CRS.parseWKT(wkt);
//		GeodeticCalculator gc = new GeodeticCalculator(crs);
//		gc.setStartingPosition( JTS.toDirectPosition(  DistanceOp.closestPoints(geo, p)[0], crs ) );
//		gc.setDestinationPosition( JTS.toDirectPosition( c, crs ) );
//
//		double distance = gc.getOrthodromicDistance();
//		System.out.println(distance);
		
		return collection;
	}
	    
//	public Point findNearestPolygon(Point p) {
//	        final double MAX_SEARCH_DISTANCE = index.getBounds().getSpan(0);
//	        Coordinate coordinate = p.getCoordinate();
//	        ReferencedEnvelope search = new ReferencedEnvelope(new Envelope(coordinate),
//	            index.getSchema().getCoordinateReferenceSystem());
//	        search.expandBy(MAX_SEARCH_DISTANCE);
//	        BBOX bbox = ff.bbox(ff.property(index.getSchema().getGeometryDescriptor().getName()), (BoundingBox) search);
//	        SimpleFeatureCollection candidates = index.subCollection(bbox);
//
//	        double minDist = MAX_SEARCH_DISTANCE + 1.0e-6;
//	        Coordinate minDistPoint = null;
//	        try (SimpleFeatureIterator itr = candidates.features()) {
//	          while (itr.hasNext()) {
//
//	            SimpleFeature feature = itr.next();
//	            LocationIndexedLine line = new LocationIndexedLine(((MultiPolygon) feature.getDefaultGeometry()).getBoundary());
//	            LinearLocation here = line.project(coordinate);
//	            Coordinate point = line.extractPoint(here);
//	            double dist = point.distance(coordinate);
//	            if (dist < minDist) {
//	              minDist = dist;
//	              minDistPoint = point;
//	              lastMatched = feature;
//	            }
//	          }
//	        }
//	        Point ret = null;
//	        if (minDistPoint == null) {
//	          ret = gf.createPoint((Coordinate) null);
//	        } else {
//	          ret = gf.createPoint(minDistPoint);
//	        }
//	        return ret;
//	      }
//
//		public SimpleFeature getLastMatched() {
//	        return lastMatched;
//	    }


	
	
	public void getDistancetoClosestRoad(double lat,  double lon) {
		
		
	}

}
