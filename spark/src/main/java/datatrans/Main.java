package datatrans;

public class Main {
	
	public static void main(String[] args) throws Exception {
		double lon = -80.482415;
		double lat = 36.361;
		
		NearestRoad nr = new NearestRoad("/Users/lisa/RDP_Share/GIS/tl_2015_allstates_prisecroads_lcc.shp");
		double distance = nr.getMinimumDistance(lat, lon);
		System.out.println("Nearest primary or secondary road, " + nr.getMatchedRoadName() + ", is " + (int)distance + " meters away from point " + lon + ", " + lat);
		
		GeoidFinder gf = new GeoidFinder("/Users/lisa/RDP_Share/GIS/census_tracts/tl_2016_37_bg_lcc.shp");
		String geoid = gf.getGeoidForLatLon(lat, lon);
		System.out.println("GEOID = " + geoid);
	}
}
