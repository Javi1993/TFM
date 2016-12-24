package preprocesamiento;

import gov.nasa.worldwind.avlist.AVKey;
import gov.nasa.worldwind.geom.LatLon;
import gov.nasa.worldwind.geom.coords.UTMCoord;

public class Borrame {

	public static void main(String[] args) {
		LatLon test = UTMCoord.locationFromUTMCoord(30, AVKey.NORTH, 440001.929, 4475627.276);
		System.out.println("LAT: "+test.getLatitude()+" LONG: "+test.getLongitude());
	}

}
