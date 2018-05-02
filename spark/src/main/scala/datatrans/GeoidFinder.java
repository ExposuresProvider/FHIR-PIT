package datatrans;

import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Geometry;

public class GeoidFinder {
	
	private GeometryFactory gf = new GeometryFactory();
	private SpatialIndexFeatureCollection index;
	private final String geoidPrefix = "15000US";
	
	public GeoidFinder() {
		// try and get shapefile path out of config file here?
	}
	
	public GeoidFinder(String blockgrpShapefilePath) {
		try {		
			ShapefileHandler shp = new ShapefileHandler(blockgrpShapefilePath);
			SimpleFeatureCollection features = shp.getFeatureCollection();
			//System.out.println("Shapefile Collection: " + shp.getFeatureCollection().size() + " features");
			index = new SpatialIndexFeatureCollection(features.getSchema());
			index.addAll(features);
		}
		catch(Exception e) {
			// write to logfile instead??
			System.out.println(e);
		}
	}
	
	private SimpleFeature getCensusBlockContainingPoint(Point p) {
		
		SimpleFeature feature = null;

	    try (SimpleFeatureIterator itr = index.features()) {
	    	
	    	while (itr.hasNext()) {
	    		feature = itr.next();
	    		Geometry geom = (Geometry) feature.getDefaultGeometry();
	    		if (geom.contains(p)) {
	    			break;
	    			
	    		}
	    	}
	    }
	    catch(Exception e) {
	    	System.out.println(e);
	    }
		
		return feature;
	}
	
	private Point createPointLCC(double lat, double lon) throws TransformException, FactoryException {
		com.vividsolutions.jts.geom.Point p_lcc = null;
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
	
	public String getGeoidForLatLon(double lat, double lon) {
		
		String geoid = null;
		Point p = null;
		
		try {
			p = createPointLCC(lat, lon);
		}
		catch(Exception e) {
			// write to logfile instead??
			System.out.println(e);
		}
		
		SimpleFeature feature = getCensusBlockContainingPoint(p);
		String id = (String) feature.getAttribute("GEOID");	
		geoid = geoidPrefix + id;
		
		return geoid;
	}

}
