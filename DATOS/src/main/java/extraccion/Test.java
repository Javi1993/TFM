package extraccion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.EnumUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Test {

	public static void main (String[] args) throws Exception{
		Test t = new Test();
		t.getDatosGobEs();
	}

	public enum Meses {
		ENERO,
		FEBRERO,
		MARZO,
		ABRIL,
		MAYO,
		JUNIO,
		JULIO,
		AGOSTO,
		SEPTIEMBRE,
		OCTUBRE,
		NOVIEMBRE,
		DICIEMBRE;
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
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 
	 * @param tr
	 * @return
	 */
	private int checkExtension(Elements tr, String format) {
		for(Element t:tr){
			if(t.select("th").text().equals("Formato") && t.select("td").text().equals(format)){
				return tr.indexOf(t);
			}
		}
		return -1;
	}

	/**
	 * 
	 * @param url
	 * @return
	 */
	private Elements getRows(String url) {
		Elements tr = null;
		try {
			Document doc = Jsoup.connect(url).get();
			Element content = doc.select("div#content").first();
			Element table = content.select("table").first();
			tr = table.select("tr");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tr;
	}

	private String getTitle(String url) {
		String title = "";
		try {
			Document doc = Jsoup.connect(url).get();
			Element content = doc.select("div#content").first();
			title = content.select("h1").first().text();
			if(Integer.parseInt(title)>=0){
				return "..:anio:..";
			}
			return "";
		}catch (NumberFormatException e) {//limpiamos titulo si tiene numero para no coger de otras fechas
			title = title.replaceAll("\\d|\\s|\\.","");
			if(EnumUtils.isValidEnum(Meses.class, title.toUpperCase())){
				return "..:mes:..";
			}
			return title;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return title;
	}

	/**
	 * 
	 */
	public void getDatosGobEs(){
		try{
			String[] ids = getIdsDataGob();//creamos el array con todas las IDs de los datasheet a bajar de datos.gob.es	
			if(ids!=null){
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

					if(!downloadDS(urls, "CSV")){//comprobamos si lo tienen en CSV
						downloadDS(urls, "XLS");//sino se baja version en XLS
						//CONSIDERAR OTRAS VERSIONES COMO XML, TEXT PLAIN ETC
					}
				}
				System.out.println("Descarga finalizada.");
			}
		}catch (JSONException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param urls
	 * @param format
	 * @return
	 */
	private boolean downloadDS(JSONArray urls, String format){
		try{
			List<String> titulos = new ArrayList<>();
			for(int j = 0; j<urls.length(); j++){//recorremos las URLs de los datasheet para el ID actual	
				Elements tr = getRows(urls.get(j).toString());//cogemos la tabla de datos del datasheet (formato y URL)
				String title = getTitle(urls.get(j).toString());//cogemos el titulo del datasheet actual
				if(!tr.isEmpty()&&!title.equals("")){
					int pos = checkExtension(tr, format);
					if(pos>=0&&checkTitleList(titulos, title)){//es el formato deseado y no se ha descargado uno con el mismo nombre de diferente fecha
						if(tr.get(pos-1).select("th").text().equals("URL")){
							String link  = tr.get(pos-1).select("td").select("a").attr("href");//cogemos el link de descargs
							//Descargamos fichero de datos
							File csv = new File(".\\documents\\"+link.substring(link.lastIndexOf('/') + 1));
							FileUtils.copyURLToFile(new URL(link), csv);
							System.out.println("Se ha descargado "+link.substring(link.lastIndexOf('/') + 1)+".");
							titulos.add(title);
						}
					}
				}else{
					System.out.println("BOLRRAR");
				}
			}
			if(!titulos.isEmpty()){ 
				titulos.clear();
				return true; 
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private boolean checkTitleList(List<String> titulos, String title){
		for(String t:titulos){
			if(t.equals(title)){
				return false;
			}
		}
		return true;
	}
}