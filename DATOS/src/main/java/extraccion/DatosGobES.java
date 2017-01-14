package extraccion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import funciones.Funciones;

@SuppressWarnings("deprecation")
public class DatosGobES {

	private HashMap<String, String> dataset_ID = new HashMap<String, String>();//cada dataset se empareja con su ID en datos.gob.es
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

	public DatosGobES(){
		getDatosGobEs();
	}

	public HashMap<String, String> getDataset_ID(){
		return this.dataset_ID;
	}

	/**
	 * Devuelve en un array las IDs de los datasets a comprobar en datos.gob.es
	 * @return
	 */
	private String[] getIdsDataGob(){
		String  thisLine = null;
		String path = System.getProperty("extras")+"ids_datos_gob.txt";//ruta donde esta el fichero con los IDs
		int i = 0;
		try{
			String[] ids = new String[Funciones.getLineNumber(path)];//se obtiene el numero de IDs y se inicializa el array
			BufferedReader br = new BufferedReader(new FileReader(path));
			while ((thisLine = br.readLine()) != null) {//por cada ID se añade al array
				ids[i] = thisLine;
				i++;
			}
			br.close();
			return ids;//se devuelve el array
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Comprueba si el dataset a descargar es del formato deseado
	 * @param tr - Filas de la tabla HTML donde esta la info
	 * @param format - Formato buscado
	 * @return - Indice de la tabla donde esta el formato (si es el buscado)
	 */
	private int checkExtension(Elements tr, String format) {
		for(Element t:tr){
			if(t.select("th").text().equals("Formato") && t.select("td").text().equals(format)) return tr.indexOf(t);
		}
		return -1;
	}

	/**
	 * Obtiene las filas de la tabla HTML donde esta la info del dataset
	 * @param url - url a consultar
	 * @return - filas de la tabla HTML
	 * @throws IOException
	 * @throws JSONException
	 */
	private Elements getRows(String url) throws IOException, JSONException {
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;
		if (defaultValidator.isValid(url)) {//se comprueba que es valida la url
			doc = Jsoup.connect(url).get();
		}else{//la url pasada esta embebida en un JSON, se extrae
			JSONObject jsonObj = new JSONObject(url);
			doc = Jsoup.connect(jsonObj.getString("_about")).get();
		}	
		Element content = doc.select("div#content").first();//se localiza la tabla y se devuelven sus filas
		Element table = content.select("table").first();
		return table.select("tr");
	}

	/**
	 * Devuelve el titulo del dataset a descargar. Se comprueba si contiene fechas para no descargar durante la extraccion datasets similares de fechas antiguas 
	 * @param url - url a consultar
	 * @return titulo
	 * @throws IOException
	 * @throws JSONException
	 */
	private String getTitle(String url) throws IOException, JSONException {
		String title = null;
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;
		if (defaultValidator.isValid(url)) {//se comprueba que es valida la url
			doc = Jsoup.connect(url).get();
		}else{//la url pasada esta embebida en un JSON, se extrae
			JSONObject jsonObj = new JSONObject(url);
			doc = Jsoup.connect(jsonObj.getString("_about")).get();
		}	
		Element content = doc.select("div#content").first();
		title = content.select("h1").first().text();//se obtiene el titulo del dataset
		if(NumberUtils.isNumber(title)){//es unicamente un numero
			return "..:anio:..";
		}else{//tiene meses en su titulo
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
		}
		return title;
	}

	/**
	 * Comprueba que el titulo del dataset a descargar no haya sido bajado ya para otra fecha mas actual
	 * @param titulos - lista con los titulos asociados a la ID de ese dataset
	 * @param title - titulo del dataset a consultar
	 * @return
	 */
	private boolean checkTitleList(List<String> titulos, String title){
		for(String t:titulos){//se recorre la lista
			if(t.equals(title)){//hay coincidencia
				return false;
			}
		}
		return true;
	}

	/**
	 * Descarga el dataset asociado a las urls pasadas
	 * @param urls - array con urls
	 * @param format - formato de descarga deseado
	 * @param ID - ID de los datasets asociados a esas url
	 * @return
	 */
	private boolean downloadDS(JSONArray urls, String format, String ID){
		List<String> titulos = new ArrayList<>();
		boolean fileDown = false;
		String url = null;
		for(int j = 0; j<urls.length(); j++){//se recorre las URLs asociadas a esa ID
			try{
				url = urls.get(j).toString();
				Elements tr = getRows(url);//se coge las filas de la tabla de datos del dataset (formato y URL de descarga)
				String title = getTitle(url);//se coge el titulo del dataset actual
				if(tr!=null && !tr.isEmpty() && !title.equals("")){
					int pos = checkExtension(tr, format);//se comprueba el formato
					if(pos>=0 && checkTitleList(titulos, title)){//es el formato deseado y no se ha descargado un dataset con el mismo nombre de diferente fecha
						if(tr.get(pos-1).select("th").text().equals("URL")){//se busca la url de descarga
							String link  = tr.get(pos-1).select("td").select("a").attr("href");//se coge el link de descarga
							if(!Funciones.checkNew(link.substring(link.lastIndexOf('/') + 1))){//Se descarga el fichero previa comprobacion de que no este en el historico
								File fileDest = new File(System.getProperty("documents")+link.substring(link.lastIndexOf('/') + 1));
								FileUtils.copyURLToFile(new URL(link), fileDest, 5000, 30000);//se descarga el contenido y se guarda en el fichero
								titulos.add(title);//se añade el titulo a la lista
								System.out.println("Se ha descargado '"+link.substring(link.lastIndexOf('/') + 1)+"'.");
								dataset_ID.put(link.substring(link.lastIndexOf('/') + 1), ID);//se añade la ID asociada al dataset descargado
								fileDown = true;//se marca como descargado
							}else{
								titulos.add(title);//se añade el titulo a la lista
								fileDown = true;//se marca como ya descargado
							}
						}
					}
				}
			} catch (JSONException e) {
				System.err.println("Error en el JSON de la ID '"+ID+"': "+e.getMessage()+"\n"+urls.toString());
			} catch (SocketTimeoutException e) {
				System.err.println("Se ha excedido el tiempo de descarga de un dataset de la ID '"+ID+"': "+e.getMessage());
			} catch (MalformedURLException e) {
				System.err.println("La url '"+url+"' usada para la descarga del dataset de la ID '"+ID+"' no es valida: "+e.getMessage());
			} catch (IOException e) {
				System.err.println("Un dataset que pertenece a la ID '"+ID+"' está inaccesible en estos momentos: "+e.getMessage());
			}
		}
		titulos.clear();
		if(fileDown){return true;}//descarga/s correcta/s
		return false;
	}

	/**
	 * Lanza el proceso de extraccion de los datasets de www.datos.gob.es
	 */
	private void getDatosGobEs(){
		String[] ids = getIdsDataGob();//se crea el array con todas las IDs de los datasets a bajar de datos.gob.es
		if(ids!=null){
			for(int i = 0; i<ids.length; i++){//se recorre el array 
				try{
					if(!ids[i].startsWith("//")){//las IDs 'comentadas' se ignoran
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
						JSONObject jsonObj = new JSONObject(result.toString());//Se obtiene el JSON con las URL de los datasets que coinciden con el criterio de busqueda
						JSONArray urls = jsonObj.getJSONObject("result").getJSONArray("items");
						if(!downloadDS(urls, "CSV", ids[i])){//Se descargan solo los datasets en formato CSV
							downloadDS(urls, "XLS", ids[i]);//si no existe ninguno en ese formato se bajan las url con datasets en formato xls
						}
					}
				}catch (JSONException e) {
					System.err.println("El formato de JSON devuelvto por Datos.Gob.es no es valido para la ID '"+ids[i]+"'.");
				} catch (MalformedURLException e) {
					System.err.println("La URL para solicitar el dataset cuya ID es '"+ids[i]+"' es incorrecta.");
				} catch (IOException e) {
					System.err.println("El servicio de Datos.Gob.es no está disponible para la ID '"+ids[i]+"'.");
				}
			}
			System.out.println("Descarga de 'http://datos.gob.es/' finalizada.");
		}
	}
}
