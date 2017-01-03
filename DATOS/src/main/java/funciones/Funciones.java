package funciones;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Funciones {

	/**
	 * Devuelve el numero de lineas del fichero pasado
	 * @param path - localizacion del fichero
	 * @return Numero de lineas
	 * @throws IOException
	 */
	public static int getLineNumber(String path) {
		int nlines = 0;
		try{
			LineNumberReader lnr = new LineNumberReader(new FileReader(new File(path)));	
			lnr.skip(Long.MAX_VALUE);
			nlines = lnr.getLineNumber() + 1; //Add 1 because line index starts at 0
			lnr.close();
		}catch (IOException e) {
			System.err.println("No se encuentra el archivo '"+path+"'.");
		}
		return nlines;
	}

	/**
	 * Devuelve la similitud entre dos String en base a la distancia Levenshtein
	 * @param s1 - string 1
	 * @param s2 - string 2
	 * @return Similitud entre 0-1
	 */
	public static double similarity(String s1, String s2) {
		String longer = s1.toLowerCase(), shorter = s2.toLowerCase();
		if (s1.length() < s2.length()) { // longer should always have greater length
			longer = s2.toLowerCase(); shorter = s1.toLowerCase();
		}
		int longerLength = longer.length();
		if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
		return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) / (double) longerLength;
	}

	/**
	 * Comprueba si el codigo postal pasado es de la ciudad de Madrid
	 * @param CP - Codigo postal
	 * @return - booleano con respuesta
	 * @throws IOException
	 */
	public static boolean checkCP(String CP) throws IOException{
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;
		String url = "http://www.codigospostales.com/mapa.cgi?codigo="+CP;
		try{
			if (defaultValidator.isValid(url)) {
				doc = Jsoup.connect(url).timeout(30000).get();
				Element content = doc.select("h1").first();
				String[] texto = content.text().split(",");
				String city = texto[texto.length-1].trim();
				if(city.toLowerCase().equals("madrid")){
					return true;
				}
			}
		}catch (SocketTimeoutException e) {
			System.err.println("Se ha excedido el tiempo para validar el codigo postal '"+CP+"' en la URL '"+url+"'.");
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Inserta la geolocalizacion en un documento JSON
	 * @param doc - documento JSON
	 * @param lat - latitud
	 * @param lon - longitud
	 */
	@SuppressWarnings("serial")
	public static void setCoordinates(org.bson.Document doc, double lat, double lon){
		doc.append("geo", new org.bson.Document("type","Point")
				.append("coordinates", new ArrayList<Double>(){{
					add(lon);
					add(lat);
				}}));
	}
}
