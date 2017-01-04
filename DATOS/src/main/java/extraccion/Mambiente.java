package extraccion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import com.csvreader.CsvWriter;
import funciones.Funciones;

public class Mambiente {

	public Mambiente(){
		String  thisLine = null;
		String path = ".\\extras\\url_mambiente.txt";//documento con las URLs de los datos
		int i = 0;
		try{
			String[] urls = new String[Funciones.getLineNumber(path)];
			BufferedReader br = new BufferedReader(new FileReader(path));
			while ((thisLine = br.readLine()) != null) {
				urls[i] = thisLine;
				i++;
			}
			br.close();
			calidadAire(urls[0]);
			calidadAcustica(urls[1]);
			System.out.println("Descarga de 'http://www.mambiente.munimadrid.es' finalizada.");
		}catch(FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Lee de la URL pasada la informacion medida por la estaciones de calidad del aire
	 * @param url - URL donde estan los datos
	 * @throws IOException
	 */
	private void calidadAire(String url) throws IOException{
		List<HashMap<String, String>> estaciones = new ArrayList<HashMap<String, String>>();
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;
		String fechaHora = null;
		if (defaultValidator.isValid(url)) {
			doc = Jsoup.connect(url).timeout(30000).get();
			Element content = doc.select("table.inf_diario").first();
			fechaHora = content.select("caption").select("span.tabla_titulo").select("span.tabla_titulo_hora").text();//cogemos la fecha y hora
			if(!Funciones.checkNew(fechaHora.replaceAll(":", "-")+"_calidad-aire.csv")){
				HashMap<String, List<String>> infoEstacion = leerExcelEstaciones("estaciones-control-aire");
				Element table = content.select("tbody").first();
				Elements tableContent = table.select("tr");
				for(Element tc:tableContent){
					if(!tc.className().equals("thcabecera") && !tc.className().equals("separador")){//nueva estacion
						HashMap<String, String> cabeceras = new HashMap<String, String>();
						cabeceras.put("fecha", fechaHora);
						Elements columns = tc.select("td");
						for(Element c:columns){
							cabeceras.put(c.attr("headers"), c.text());
						}
						completarEstacion(cabeceras, infoEstacion, 0);
						estaciones.add(cabeceras);
					}
				}
				volcarCSV(estaciones, fechaHora.replaceAll(":", "-")+"_calidad-aire.csv");
				System.out.println("Se ha descargado los valores de calidad del aire a fecha de "+fechaHora);
			}
		}
	}

	/**
	 * Lee de la URL pasada la informacion medida por la estaciones acusticas
	 * @param url - URL donde estan los datos
	 * @throws IOException
	 */
	private void calidadAcustica(String url) throws IOException{
		List<HashMap<String, String>> estaciones = new ArrayList<HashMap<String, String>>();
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;
		String fecha = null;
		if (defaultValidator.isValid(url)) {
			doc = Jsoup.connect(url).timeout(30000).get();
			Element content = doc.select("table.inf_diario").first();
			fecha = content.select("caption").select("span.tabla_titulo").select("span.tabla_titulo_fecha").text();//cogemos la fecha
			if(!Funciones.checkNew(fecha.replaceAll(":", "-")+"_calidad-acustica.csv")){
				HashMap<String, List<String>> infoEstacion = leerExcelEstaciones("estaciones-acusticas");
				Element table = content.select("tbody").first();
				Elements tableContent = table.select("tr");
				for(Element tc:tableContent){
					if(!tc.className().equals("thcabecera") && !tc.className().equals("separador")){//nueva estacion
						HashMap<String, String> cabeceras = new HashMap<String, String>();
						cabeceras.put("fecha", fecha);
						Elements columns = tc.select("td");
						for(Element c:columns){
							cabeceras.put(c.attr("headers"), c.text());
						}
						completarEstacion(cabeceras, infoEstacion, 1);
						estaciones.add(cabeceras);
					}
				}
				volcarCSV(estaciones, fecha.replaceAll(":", "-")+"_calidad-acustica.csv");
				System.out.println("Se ha descargado los valores de calidad acústica a fecha de "+fecha);	
			}
		}
	}

	/**
	 * Vuelca en un CSV con la estructa deseada la informacion completa de las estaciones de calidad aire y acustica
	 * @param estaciones - Estrucutra con la informacion de las estaciones
	 * @param name - Acustica o aire
	 */
	private void volcarCSV(List<HashMap<String, String>> estaciones, String name) {
		String outputFile = "./documents/"+name;
		try {
			if(estaciones != null && !estaciones.isEmpty()){
				CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, false), ';');
				HashMap<String, String> headers = estaciones.get(0);
				for (String head: headers.keySet()){
					csvOutput.write(head.toString());
				}
				csvOutput.endRecord();
				for(HashMap<String,String> hm:estaciones){
					for (String valor: hm.keySet()){
						csvOutput.write(hm.get(valor).toString());
					}
					csvOutput.endRecord();
				} 
				csvOutput.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Busca en la estructura pasada el atibuto equivalente al que se quiere almacenar en el JSON
	 * @param infoEstacion - Estrcuta con la informacion de la estacion
	 * @param aprox - nombre aproximado del atributo buscado en la estructura
	 * @param tipo - tipo de estacion (aire/acustica)
	 * @return
	 */
	private String buscarValor(Set<String> infoEstacion, String aprox, int tipo) {
		double max = 0.0;
		double aux = 0.0;
		String header = null;
		//nombres con problemas para detectar por Levenshtein Distance
		if(aprox.equals("PUENTE DE VALLECAS") && tipo == 0) return "Vallecas";
		if(aprox.equals("RETIRO") && tipo == 0) return "Parque del Retiro";
		if(aprox.equals("GLORIETA CARLOS V") && tipo == 1) return "Carlos V";
		if(aprox.equals("MARAÑON") && tipo == 1) return "Gregorio Marañón";
		if(aprox.equals("MORATALAZ R") && tipo == 1) return "Avda Moratalaz";
		if(aprox.equals("URBANIZACION EMBAJADA") && tipo == 1) return "Urb Embajada 2";
		for(String cab:infoEstacion){
			if( (aux = Funciones.similarity(cab, aprox)) > max && aux > 0.55){
				max = aux;
				header = cab;
			}
		}
		return header;
	}

	/**
	 * Dada la informacion de las estaciones y sus valores medidos junta todo en una unica estructura
	 * @param cabeceras - Estrcutura con los valores medidos por la estacion
	 * @param infoEstacion - Estrcutura con la informacion basica de la estacion
	 * @param tipo - indica si es de aire o acustica
	 */
	private void completarEstacion(HashMap<String, String> cabeceras, HashMap<String, List<String>> infoEstacion, int tipo) {
		HashMap<String, String> cabecerasAux = new HashMap<String, String>(cabeceras);
		for (String cb: cabecerasAux.keySet()){
			if(cb.toLowerCase().equals("estación")||cb.toLowerCase().equals("estacion")){
				String attr = buscarValor(infoEstacion.keySet(), cabecerasAux.get(cb), tipo);
				if(attr!=null){
					cabeceras.put("numero", ((List<String>)infoEstacion.get(attr)).get(0));
					cabeceras.put("longitud", ((List<String>)infoEstacion.get(attr)).get(1));
					cabeceras.put("latitud", ((List<String>)infoEstacion.get(attr)).get(2));
				}
			}
		}
	}

	/**
	 * Lee el XLS del listado de estaciones acusticas/aire
	 * @param doucment - String con identificador de si son de aire o acusticas
	 * @return Hashmap de 'nombre estacion' como key y sus atributos como value.
	 */
	private HashMap<String, List<String>> leerExcelEstaciones(String doucment){
		HashMap<String, List<String>> infoAux = new HashMap<String, List<String>>();
		try {
			File dir = new File("./documents/");
			FileFilter fileFilter = new WildcardFileFilter("*"+doucment+".*");
			POIFSFileSystem fs = new POIFSFileSystem(new FileInputStream(dir.listFiles(fileFilter)[0].getAbsolutePath()));
			HSSFWorkbook wb = new HSSFWorkbook(fs);
			HSSFSheet sheet = wb.getSheetAt(0);
			HSSFRow row;
			HSSFCell cell;
			boolean contenidoBueno = false;
			short longitud = -1;
			int columnIndexNum = 0;
			int columnIndexrName = 0;
			int columnLongNum = 0;
			int columnLatrName = 0;
			int rowIndexHeaders = 0;
			int rows; // No of rows
			rows = sheet.getPhysicalNumberOfRows();
			int cols = 0; // No of columns
			int tmp = 0;

			// This trick ensures that we get the data properly even if it doesn't start from first few rows
			for(int i = 0; i < 10 || i < rows; i++) {
				row = sheet.getRow(i);
				if(row != null) {
					tmp = sheet.getRow(i).getPhysicalNumberOfCells();
					if(tmp > cols) cols = tmp;
				}
			}
			for(int r = 0; r < rows; r++) {
				row = sheet.getRow(r);
				if(row != null && (longitud == -1|| row.getLastCellNum()==longitud)) {
					List<String> estacion = new ArrayList<String>();
					String nombre = "";
					for(int c = 0; c < cols; c++) {
						cell = row.getCell((short)c);
						if(cell != null && (cell.toString().toLowerCase().equals("número") || cell.toString().toLowerCase().equals("nº") || contenidoBueno)) {
							if((cell.toString().toLowerCase().equals("número") || cell.toString().toLowerCase().equals("nº")) && !contenidoBueno){
								longitud = row.getLastCellNum();
								contenidoBueno = true;
								columnIndexNum = cell.getColumnIndex();
								rowIndexHeaders = cell.getRowIndex();
							}else if(cell.toString().toLowerCase().equals("estación") || cell.toString().toLowerCase().equals("nombre")){
								columnIndexrName = cell.getColumnIndex();
							}else if(cell.toString().toLowerCase().equals("longitud")){
								columnLongNum = cell.getColumnIndex();
							}else if(cell.toString().toLowerCase().equals("latitud")){
								columnLatrName = cell.getColumnIndex();
							}else if(cell.getRowIndex()>rowIndexHeaders){//entramos en contenido
								if(cell.getColumnIndex() == columnIndexNum){
									estacion.add(String.valueOf(cell.getNumericCellValue()));
								}else if(cell.getColumnIndex() == columnIndexrName){
									nombre = cell.getStringCellValue();
								}else if(cell.getColumnIndex() == columnLongNum || cell.getColumnIndex() == columnLatrName){
									estacion.add(String.valueOf(cell.getStringCellValue()));
								}
							}		
						}
					}
					infoAux.put(nombre, estacion);
				}
			}
			wb.close();
			fs.close();
		} catch(Exception ioe) {
			ioe.printStackTrace();
		}
		return infoAux;
	}
}
