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
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import preprocesamiento.Almacenar;
import preprocesamiento.Limpieza;

public class DatosGobES {

	private String path = "."+File.separator+"documents";
	private enum Meses {
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

	private HashMap<String, String> dataset_ID = new HashMap<String, String>();//por cada dataset empareja con su ID
	private HashMap<String, String> getDataset_ID(){
		return this.dataset_ID;
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
	 * @throws IOException 
	 * @throws JSONException 
	 */
	private Elements getRows(String url) throws IOException, JSONException {
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;

		if (defaultValidator.isValid(url)) {
			doc = Jsoup.connect(url).get();
		}else{
			JSONObject jsonObj = new JSONObject(url);
			doc = Jsoup.connect(jsonObj.getString("_about")).get();
		}	
		Element content = doc.select("div#content").first();
		Element table = content.select("table").first();
		return table.select("tr");
	}

	private String getTitle(String url) {
		String title = "";
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;
		try {
			if (defaultValidator.isValid(url)) {
				doc = Jsoup.connect(url).get();
			}else{
				JSONObject jsonObj = new JSONObject(url);
				doc = Jsoup.connect(jsonObj.getString("_about")).get();
			}	
			Element content = doc.select("div#content").first();
			title = content.select("h1").first().text();
			if(Integer.parseInt(title)>=0){
				return "..:anio:..";
			}
			return "";
		}catch (NumberFormatException e) {//limpiamos titulo si tiene numero o mes para no coger de otras fechas anteriores
			title = title.replaceAll("\\d|\\s|\\.","");
			if(EnumUtils.isValidEnum(Meses.class, title.toUpperCase())){
				return "..:mes:..";
			}else{
				for(Meses mes:Meses.values()){
					if(title.toUpperCase().startsWith(mes.toString())){
						return "..:mes:..";
					}
				}
			}
			return title;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return title;
	}

	/**
	 * 
	 */
	public void getDatosGobEs(){
		try{
			String[] ids = getIdsDataGob();//creamos el array con todas las IDs de los datasets a bajar de datos.gob.es
			if(ids!=null){
				for(int i = 0; i<ids.length; i++){//recorremos el array 
					if(!ids[i].startsWith("//")){//las IDs 'comentadas' se ignoran
						//Obtenemos el JSON con la URL de los dataset que coinciden con el criterio de busqueda
						StringBuilder result = new StringBuilder();
						URL url = new URL("http://datos.gob.es/apidata/catalog/distribution/dataset/"+ids[i]+"?_sort=-title&_pageSize=20&_page=0");
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

						if(!downloadDS(urls, "CSV", ids[i])){//comprobamos si lo tienen en CSV
							downloadDS(urls, "XLS", ids[i]);//sino se baja version en XLS
							//CONSIDERAR OTRAS VERSIONES COMO XML, TEXT PLAIN ETC
						}
					}
				}
				System.out.println("Descarga finalizada.");
				Limpieza pre = new Limpieza();
				pre.separacionCarpetas(path);
//			    Iterator it = getDataset_ID().entrySet().iterator();
//			    while (it.hasNext()) {
//			        Map.Entry pair = (Map.Entry)it.next();
//			        System.out.println(pair.getKey() + " = " + pair.getValue());
//			        it.remove(); // avoids a ConcurrentModificationException
//			    }
				Almacenar alm = new Almacenar(getDataset_ID());



				//				System.out.println("_--------------------------------------------------_");
				//				Set set = ID_datasets.entrySet();
				//				Iterator iterator = set.iterator();
				//				while(iterator.hasNext()) {
				//					Map.Entry mentry = (Map.Entry)iterator.next();
				//					System.out.println("key is: "+ mentry.getKey() + " & Value is: ");
				//					for(String dataset:((List<String>)mentry.getValue())){
				//						System.out.println(dataset);
				//					}
				//					System.out.println();
				//				}
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
	private boolean downloadDS(JSONArray urls, String format, String ID){
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
							//System.out.println(link);
							FileUtils.copyURLToFile(new URL(link), csv, 5000, 30000);
							System.out.println("Se ha descargado "+link.substring(link.lastIndexOf('/') + 1)+".");
							titulos.add(title);
							dataset_ID.put(link.substring(link.lastIndexOf('/') + 1), ID);
						}
					}
				}else{
					System.out.println("*********************BORRAR**********************");
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			System.err.println("Se ha excedido el tiempo para descargar un dataset de la ID '"+ID+"'.");
			//			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Un dataset de la ID '"+ID+"' está inaccesible en estos momentos.");
			//			e.printStackTrace();
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

	public static void main (String[] args) throws Exception{
		DatosGobES t = new DatosGobES();
		t.getDatosGobEs();
	}
}
