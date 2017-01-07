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

import funciones.Funciones;

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
		String cp;
		if((cp = locCP.get(street))!=null){//ya se ha comprobado esta calle/lugar
			if(cp.equals("")) return null;
			return cp;
		}else{
			String dir = "https://maps.googleapis.com/maps/api/geocode/json?address="+street+"+MADRID";
			cp = doRequest(dir, 0);
			if(cp!=null){
				locCP.put(street, cp);
			}else{
				locCP.put(street, "");	
			}
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
			System.err.println(e.getMessage());
			//			e.printStackTrace();
		} catch (JSONException e) {
			try {
				if(jsonObj.get("status").equals("OVER_QUERY_LIMIT") && Funciones.getLineNumber("."+File.separator+"extras"+File.separator+"google-keys")<number)
					return doRequest(dir, number++);
			} catch (JSONException e1) {
				System.err.println("No existe código postal para la url pasada ('"+dir+"') o se ha superado el límite de peticiones para las key.");
				System.err.println(jsonObj.toString());
				return null;
			}
		}
		return null;
	}

	/**
	 * Obtiene una API key de la lista para usarla en las peticiones
	 * @param number - numero de linea de la API key a coger
	 * @return Devuelve la key
	 * @throws IOException
	 */
	private String getKey(int number) throws IOException{
		Stream<String> lines = Files.lines(Paths.get("."+File.separator+"extras"+File.separator+"google-keys"));//abrimos el fichero de keys
		String line = lines.skip(number).findFirst().get();//obtenemos la linea
		lines.close();
		if(!line.equals("")){
			line = "&key="+line;
		}
		return line;
	}
}
