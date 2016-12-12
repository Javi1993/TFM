package extraccion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Test {

	public static void main (String[] args) throws Exception{
		Test t = new Test();
		t.testGet();
	}

	/**
	 * 
	 * @return
	 */
	private String[] getIdsDataGob(){
		String[] ids = new String[getLineNumber()];
		String  thisLine = null;
		int i = 0;
		try{
			// open input stream test.txt for reading purpose.
			BufferedReader br = new BufferedReader(new FileReader(".\\extras\\ids_datos_gob.txt"));
			while ((thisLine = br.readLine()) != null) {
				ids[i] = thisLine;
				i++;
			}
			br.close();
			return ids;
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @return
	 */
	private int getLineNumber(){
		LineNumberReader lnr;
		try {
			lnr = new LineNumberReader(new FileReader(new File(".\\extras\\ids_datos_gob.txt")));	
			lnr.skip(Long.MAX_VALUE);
			int nlines = lnr.getLineNumber() + 1; //Add 1 because line index starts at 0
			lnr.close();
			return nlines;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 
	 * @throws Exception
	 */
	public void testGet() throws Exception{
		String[] ids = getIdsDataGob();//creamos el array con todas las IDs de los datasheet a bajar de datos.gob.es	

		for(int i = 0; i<ids.length; i++){//recorremos el array 
			//Obtenemos el JSON con la URL de los datasheet que coinciden con el criterio de busqueda
			StringBuilder result = new StringBuilder();
			URL url = new URL("http://datos.gob.es/apidata/catalog/distribution/dataset/"+ids[i]+"?_sort=-title&_pageSize=10&_page=0");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Content-Type", "application/json");
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			rd.close();

			//guardamos los enlaces a los datasheets
			JSONObject jsonObj = new JSONObject(result.toString());
			JSONArray urls = (JSONArray) ((JSONObject)jsonObj.get("result")).get("items");
			
			for(int j = 0; j<urls.length(); j++){
				//Accedemos a las URL de los datasheets
				Document doc = Jsoup.connect(urls.get(j).toString()).get();
				Element table = doc.select("table").first();
				Elements td = table.select("td");
				
				//VER CUANDO SON XLS Y XML!! PASAR A CSV TRAS DESCARGAR LOS XLS 
				//(SI NO HA DESCARGADO EL MISMO EN FORMATO CSV TAMBIEN) CON ALGUNA FUNCION EN JAVA
				if(td.get(1).text().equals("CSV")||td.get(1).text().equals("XLS")){//cogemos solo los CSV
					Element a = td.get(0).select("a").first();
					String link  = a.attr("href");

					//Descargamos fichero con datos
					File csv = new File(".\\documents\\"+link.substring(link.lastIndexOf('/') + 1));
					FileUtils.copyURLToFile(new URL(link), csv);
					System.out.println("Se ha descargado "+link.substring(link.lastIndexOf('/') + 1)+".");
				}
			}
		}
		System.out.println("Descarga finalizada.");
	}
}