package preprocesamiento.geocoding;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Geocode {

	private HashMap<String, String> locCP;

	public Geocode(){
		locCP = new HashMap<String, String>();
	}

	public String getCPbyCoordinates(double lon, double lat){
		String dir = "https://maps.googleapis.com/maps/api/geocode/json?latlng="+lat+","+lon;
		return doRequest(dir, 0);
	}

	public String getCPbyStreet(String street){
		street = street.replaceAll("\\s", "+");
		street = street.replaceAll("Ñ", "N");
		String cp;
		if((cp = locCP.get(street))!=null){
			return cp;
		}else{
			String dir = "https://maps.googleapis.com/maps/api/geocode/json?address="+street+"+MADRID";
			cp = doRequest(dir, 0);
			locCP.put(street, cp);
			return cp;
		}
	}

	private String doRequest(String dir, int number){
		JSONObject jsonObj = null;
		try {
			StringBuilder result = new StringBuilder();
			String dirAux = dir+getKey(number);
			URL url = new URL(dirAux);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Content-Type", "application/json");
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			rd.close();
			jsonObj = new JSONObject(result.toString());
			JSONArray locs = jsonObj.getJSONArray("results").getJSONObject(0).getJSONArray("address_components");
			return locs.getJSONObject(locs.length()-1).getString("short_name").trim();
		} catch (MalformedURLException e) {
			System.err.println("La URL '"+dir+"' no es valida.");
			//			e.printStackTrace();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			//			e.printStackTrace();
		} catch (JSONException e) {
			System.err.println("No existe código postal para la localizacion pasada o se ha superado el límite de peticiones para la key.");
			System.err.println(jsonObj.toString());
			return doRequest(dir, number++);//porbamos con otra key
			//			e.printStackTrace();
		}
		return null;
	}

	private String getKey(int number) throws IOException{
		Stream<String> lines = Files.lines(Paths.get("."+File.separator+"extras"+File.separator+"google-keys"));
		String line = lines.skip(number).findFirst().get();
		lines.close();
		if(!line.equals("")){
			line = "&key="+line;
		}
		return line;
	}
}
