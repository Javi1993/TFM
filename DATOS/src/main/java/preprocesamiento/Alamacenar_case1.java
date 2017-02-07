package preprocesamiento;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.Document;
import com.csvreader.CsvReader;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import funciones.Funciones;

public class Alamacenar_case1 {

	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<Document> collection;

	public Alamacenar_case1(){
		cargarDatos();//lanza la carga y almacenamiento de datos
	}

	/**
	 * Realiza la conexion con la BBDD y la coleccion
	 */
	private void conDB(){
		client = new MongoClient(System.getProperty("server"), 27017);//conectamos
		database = client.getDatabase(System.getProperty("db"));//elegimos bbdd
		collection = database.getCollection("chicago");//tomamos la coleccion
	}

	private void cargarDatos(){
		try{
			conDB();
			collection.drop();
			generarCrimes();
			client.close();
		}catch (IOException e) {
			System.err.println("Error durante el pre-procesamiento");
		}
	}

	private List<String> getCampos() throws IOException{
		List<String> campos = new ArrayList<String>();
		byte[] encoded = Files.readAllBytes(Paths.get(System.getProperty("extras")+"JSON_esquema_case1.json"));
		Document JSON = Document.parse(new String(encoded, "ISO-8859-1"));
		for(String label:JSON.keySet()){
			campos.add(label+"&&"+JSON.get(label));
		}
		return campos;
	}

	@SuppressWarnings("deprecation")
	private String buscarValor(CsvReader csvDoc, String aprox, String tipo) throws IOException {
		String[] headers = csvDoc.getHeaders();
		double max = 0.0;
		double aux = 0.0;
		String header = null;
		for(int i = 0; i<headers.length; i++){
			if( (aux = Funciones.similarity(headers[i], aprox)) > max && aux > 0.55){
				max = aux;
				header = headers[i];
			}
		}
		String value = csvDoc.get(header);
		if(!value.equals("")){
			if(NumberUtils.isNumber(tipo)){//cogemos solo la parte numerica
				if(tipo.length()>1){//catastro
					value = value.replaceAll("\\.", "");
				}
				value = value.replaceAll("\\s+","").replaceAll(",", "\\.");
				Pattern p = Pattern.compile("(\\d+\\.\\d+)");//numero decimal
				Matcher m = p.matcher(value);
				if (m.find()) {
					return m.group(1);
				}else{
					p = Pattern.compile("(\\d+)");//numero entero
					m = p.matcher(value);
					if (m.find()) {
						return m.group(1);
					}
				}
			}
			return value.trim();
		}
		return null;
	}

	private void generarCrimes() throws IOException {
		File dir = new File(System.getProperty("documents"));
		FileFilter fileFilter = new WildcardFileFilter("Crimes*.csv");
		if(dir.exists() && dir.listFiles(fileFilter).length>0){
			CsvReader crimes_csv = new CsvReader (dir.listFiles(fileFilter)[0].getAbsolutePath(), ',');
			crimes_csv.readHeaders();
			List<String> attrPadron = getCampos();//obtenemos los campos del JSON esquema
			while (crimes_csv.readRecord()){//recorremos el CSV
				String attr = null;
				Document crime = new Document();
				String lat, lon;
				for(String label:attrPadron){
					if((attr = buscarValor(crimes_csv, label.split("&&")[0], label.split("&&")[1]+"1"))!=null && attr!="" && !label.split("&&")[0].equals("geo")){
						Funciones.setComunAttr(crime, attr, label);
					}else if(label.split("&&")[0].equals("geo") && (lat = buscarValor(crimes_csv, "latitude", "text"))!=null
							&& (lon = buscarValor(crimes_csv, "longitude", "text"))!=null){
						
						Funciones.setCoordinatesAux(crime, Double.parseDouble(lat), Double.parseDouble(lon));
					}
				}
				collection.insertOne(crime);//insertamos los crimenes con su informacion
			}
			crimes_csv.close();
			Funciones.deleteFile(dir.listFiles(fileFilter)[0]);
		}
	}
}
