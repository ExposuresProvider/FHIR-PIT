package datatrans;

import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.vividsolutions.jts.linearref.LocationIndexedLine;
import com.vividsolutions.jts.geom.Point;

public class NearestRoad {
	
	private FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
	private GeometryFactory gf = new GeometryFactory();
	private SpatialIndexFeatureCollection index;
	private SimpleFeature lastMatched;
	// make max search radius about 500 meters per karafecho
	private final double MAX_SEARCH_DISTANCE = 500;
	
	public NearestRoad() {
		// try and get shapefile path out of config file here?
	}
	
	public NearestRoad(String roadShapefilePath) {
		try {
			// roadShapefilePath = "/Users/lisa/RDP_Share/GIS/tl_2015_allstates_prisecroads_lcc.shp";
			
			ShapefileHandler shp = new ShapefileHandler(roadShapefilePath);
			SimpleFeatureCollection features = shp.getFeatureCollection();
			System.out.println("Shapefile Collection: " + shp.getFeatureCollection().size() + " features");
			index = new SpatialIndexFeatureCollection(features.getSchema());
			index.addAll(features);
		}
		catch(Exception e) {
			// write to logfile instead??
			System.out.println(e);
		}
	}
	
	public double getMinimumDistance(double lat, double lon) {
		double distance = MAX_SEARCH_DISTANCE;
		
		try {
			Point  p = createPointLCC(lat, lon);
			distance = findMinimumDistance(p);
		}
		catch(Exception e) {
			// write to logfile instead??
			System.out.println(e);
		}
		
		return distance;
	}
  
	private double findMinimumDistance(Point p) {
		
	    //final double MAX_SEARCH_DISTANCE = index.getBounds().getSpan(0);
		// make max search radius about 500 meters
		//final double MAX_SEARCH_DISTANCE = 500.0;
	    Coordinate coordinate = p.getCoordinate();
	    ReferencedEnvelope search = new ReferencedEnvelope(new Envelope(coordinate),
	        index.getSchema().getCoordinateReferenceSystem());
	    search.expandBy(MAX_SEARCH_DISTANCE);
	    BBOX bbox = ff.bbox(ff.property(index.getSchema().getGeometryDescriptor().getName()), (BoundingBox) search);
	    
	    SimpleFeatureCollection candidates = index.subCollection(bbox);   
	    //System.out.println(candidates.size());

	    double minDist = MAX_SEARCH_DISTANCE + 1.0e-6;
	    Coordinate minDistPoint = null;
	    try (SimpleFeatureIterator itr = candidates.features()) {
	    	
	    	while (itr.hasNext()) {
	    		SimpleFeature feature = itr.next();
	    		
	    		// use following 2 lines to get road name
	    		//String attribute = (String) feature.getAttribute("FULLNAME");
	    		//System.out.println(attribute);
	    		
	    		LocationIndexedLine line = new LocationIndexedLine(((Geometry)feature.getDefaultGeometry()));
	    		LinearLocation here = line.project(coordinate);
	    		Coordinate point = line.extractPoint(here);
	    		double dist = point.distance(coordinate);
	    		if (dist < minDist) {
	    			minDist = dist;
	    			minDistPoint = point;
	    			lastMatched = feature;
	    		}
	    	}
	    }
	    catch(Exception e) {
	    	System.out.println(e);
	    }
	    
//      Might need this code if minimum distance point is needed
//	    com.vividsolutions.jts.geom.Point ret = null;
//	    if (minDistPoint == null) {
//	      ret = gf.createPoint((Coordinate) null);
//	    } else {
//	      ret = gf.createPoint(minDistPoint);
//	    }
	    return minDist;
	  }

	public SimpleFeature getLastMatched() {
		return lastMatched;
	}
	
	private Point createPointLCC(double lat, double lon) throws TransformException, FactoryException {
		Point p_lcc = null;
		final String wkt = "PROJCS[\"AQMEII_CMAQ\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\","
							+ "SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],"
							+ "UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Conformal_Conic\"],"
							+ "PARAMETER[\"False_Easting\",-2556000.0],PARAMETER[\"False_Northing\",-1728000.0],"
							+ "PARAMETER[\"Central_Meridian\",-97.0],PARAMETER[\"Standard_Parallel_1\",33.0],"
							+ "PARAMETER[\"Standard_Parallel_2\",45.0],PARAMETER[\"Scale_Factor\",1.0],"
							+ "PARAMETER[\"Latitude_Of_Origin\",40.0],UNIT[\"Meter\",1.0]]";
		
		CoordinateReferenceSystem crs = CRS.parseWKT(wkt);
		CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:4326");
				
		Coordinate c = new Coordinate(lat, lon);
		MathTransform transform = CRS.findMathTransform(sourceCRS, crs);
		Coordinate targetCoordinate = JTS.transform(c, null, transform );
		p_lcc = gf.createPoint(targetCoordinate);
		
		return p_lcc;
	}
	
	public String getMatchedRoadName() {
		String roadName = null;
		
		if (lastMatched != null) {
			roadName = (String) lastMatched.getAttribute("FULLNAME");
		}
		
		return roadName;
		
	}

}

