package preprocesamiento.geocoding;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Geocode {

	public String getCPbyCoordinates(double lon, double lat){
		StringBuilder result = new StringBuilder();
		String APIkey = "AIzaSyAmj4TRmxQNJqoeLaXOSBDdDB8d_6WLGyY";
		String dir = "https://maps.googleapis.com/maps/api/geocode/json?latlng="+lat+","+lon+"&key="+APIkey;
		String CP = null;
		try {
			URL url = new URL(dir);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Content-Type", "application/json");
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			rd.close();
			JSONObject jsonObj = new JSONObject(result.toString());
			JSONArray locs = jsonObj.getJSONArray("results").getJSONObject(0).getJSONArray("address_components");
			CP = locs.getJSONObject(locs.length()-1).getString("short_name").trim();
		} catch (MalformedURLException e) {
			System.err.println("La URL '"+dir+"' no es valida.");
//			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return CP;
	}
	
	public String getCPbyStreet(String street){
		StringBuilder result = new StringBuilder();
		String APIkey = "AIzaSyAmj4TRmxQNJqoeLaXOSBDdDB8d_6WLGyY";
		street = street.replaceAll("\\s", "+");
		String dir = "https://maps.googleapis.com/maps/api/geocode/json?address="+street+"+MADRID&key="+APIkey;
		String CP = null;
		try {
			URL url = new URL(dir);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Content-Type", "application/json");
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			rd.close();
			JSONObject jsonObj = new JSONObject(result.toString());
			JSONArray locs = jsonObj.getJSONArray("results").getJSONObject(0).getJSONArray("address_components");
			CP = locs.getJSONObject(locs.length()-1).getString("short_name").trim();
		} catch (MalformedURLException e) {
			System.err.println("La URL '"+dir+"' no es valida.");
//			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return CP;
	}
}
