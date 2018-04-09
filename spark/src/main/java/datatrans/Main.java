package datatrans;

public class Main {
	
	public static void main(String[] args) throws Exception {
		double lon = -80.482415;
		//double lat = 36.352975;
		double lat = 36.361;
		NearestRoad nr = new NearestRoad("/Users/lisa/RDP_Share/GIS/tl_2015_allstates_prisecroads_lcc.shp");
		double distance = nr.getMinimumDistance(lat, lon);
		
		System.out.println("nearest road is " + distance + " meters away from point " + lon + ", " + lat);
	}

}
