package preprocesamiento;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.csvreader.CsvReader;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class Almacenar {

//	private HashMap<String, List<String>> ID_datasets;

	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<Document> collection;


	public Almacenar(/*HashMap<String, List<String>> ID_datasets*/){
		client = new MongoClient("localhost", 27017);//conectamos
		database = client.getDatabase("tfm");//elegimos bbdd
		collection = database.getCollection("distritos");//tomamos la coleccion de estaciones de aire

//		this.ID_datasets = ID_datasets;
		//		generarColeccion();
	}

	/**
	 * @throws IOException 
	 * 
	 */
	private void generarColeccion() throws IOException{
		CsvReader distritos_barrios = new CsvReader(".\\documents\\200076-1-padron.csv");
		distritos_barrios.readHeaders();

		//		private List<Document> distritos;
		List<Document> distritos = new ArrayList<Document>();
		int index = 0;
		while (distritos_barrios.readRecord()){
			if(distritos.isEmpty() || (index = buscarDistrito(distritos, distritos_barrios.get("COD_DISTRITO")))<0){
				distritos.add(new Document("_id", distritos_barrios.get("COD_DISTRITO"))
						.append("nombre", distritos_barrios.get("DESC_DISTRITO"))
						.append("barrios", new ArrayList<Document>()
								.add(new Document()
										.append("id_barrio",  distritos_barrios.get("COD_BARRIO"))//VER SI ES ESTE CODIGO O EL DE DIST_BARRIO
										.append("nombre", distritos_barrios.get("DESC_BARRIO")))));
			}else{
				Document dist = distritos.get(index);
				Document barrio = new Document()
						.append("id_barrio",  distritos_barrios.get("COD_BARRIO"))//VER SI ES ESTE CODIGO O EL DE DIST_BARRIO
						.append("nombre", distritos_barrios.get("DESC_BARRIO"));
				((List<Document>) dist.get("barrios")).add(barrio);//VER SI ASI VALE O HAY QUE MACHACAR LISTA EN DIST!!
				distritos.add(index, dist);//actualizamos
			}
			//TERMINAR AÑADIENDO PADRON!! VER ARCHIVO CON SIGNIFICADO ID EDAD ETC
		}
		distritos_barrios.close();
		collection.insertMany(distritos);//insertamos los distritos
	}

	private int buscarDistrito(List<Document> distritos, String code){
		for(Document dist:distritos){
			if(dist.getObjectId("_id").toString().equals(code)){
				return distritos.indexOf(dist);
			}
		}
		return -1;
	}

	public static void main(String[] args) throws JSONException, FileNotFoundException, IOException, ParseException  {
		Almacenar alm = new Almacenar();
		alm.generarColeccion();
	}
}
