package preprocesamiento;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
		//		client = new MongoClient("localhost", 27017);//conectamos
		//		database = client.getDatabase("tfm");//elegimos bbdd
		//		collection = database.getCollection("distritos");//tomamos la coleccion de estaciones de aire

		//		this.ID_datasets = ID_datasets;
		//		generarColeccion();
	}

	/**
	 * @throws IOException 
	 * 
	 */
	private void generarColeccion() throws IOException{
		//collection.drop();


		//EMPEZAR COGIENDO DE PK Y [GENERANDO BVARRIOS Y DISTRTIOS EN BASE A LOS LUGARES, luego ya meter lad ID de dist y barrio en los que tengan ese valor)
		File folder = new File(".\\documents\\PK_FORMAT");
		List<Document> distritos = new ArrayList<Document>();
		for (File fileEntry : folder.listFiles()) {
			if (!fileEntry.isDirectory()) {
				CsvReader distritos_zonas = new CsvReader(fileEntry.getAbsolutePath(),';');
				distritos_zonas.readHeaders();
				int index = 0;
				int[] dist_barrio_index = null;
				while (distritos_zonas.readRecord()){
					if( (dist_barrio_index = buscarDistritoBarrioInfo(distritos_zonas)) !=null ){
						if(distritos_zonas.get("PK").equals("4569832"))
						{
							System.out.println(fileEntry.getName());
						}
						if(distritos.isEmpty() || (index = buscarDistrito_Barrio(distritos, distritos_zonas.get(dist_barrio_index[0]), "nombre"))<0){//distrito nuevo
							Document dist = new Document().append("nombre", distritos_zonas.get(dist_barrio_index[0]));

							List<Document> barrios = new ArrayList<Document>();
							barrios.add(completarBarrioNuevo(distritos_zonas, dist_barrio_index[1]));
							dist.append("barrios", barrios);
							
							distritos.add(dist);	
						}else{//ese distrito ya esta guardado
							Document dist = distritos.get(index);
							List<Document> barrios = (List<Document>) dist.get("barrios");
							if(buscarDistrito_Barrio(barrios, distritos_zonas.get(dist_barrio_index[1]), "nombre")>=0){//barrio ya existente
								//								Document barrio = new Document()
								//										.append("id_barrio",  distritos_barrios.get("COD_BARRIO"))//VER SI ES ESTE CODIGO O EL DE DIST_BARRIO
								//										.append("nombre", distritos_barrios.get("DESC_BARRIO"));
								//								barrios.add(barrio);
								//								dist.replace("barrios", (List<Document>) dist.get("barrios"), barrios);
								//								distritos.remove(index);//actualizamos
								//								distritos.add(dist);
								//SI EL BARRIO EXISTE SE AÑADE UNA NUEVA ZONA(PK), AUMENTAR CP y AÑADIR A LISTA NUEVA ZONA(PK)
							}else{//barrio nuevo
								Document doc = completarBarrioNuevo(distritos_zonas, dist_barrio_index[1]);
								//								System.out.println(doc.toJson());
								barrios.add(doc);
								dist.replace("barrios", (List<Document>) dist.get("barrios"), barrios);
								distritos.remove(index);//actualizamos
								distritos.add(dist);
							}
						}
					}
				}
			}
		}

		//PK SOLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO ESTA PARTE DE ARRIBA!!!!!!!!!!!!





		//		CsvReader distritos_barrios = new CsvReader(".\\documents\\DISTRICT_BARRIO_FORMAT\\200076-1-padron.csv",';');
		//		distritos_barrios.readHeaders();
		//
		//		//		private List<Document> distritos;
		//		List<Document> distritos = new ArrayList<Document>();
		//		int index = 0;
		//		while (distritos_barrios.readRecord()){
		//			if(distritos.isEmpty() || (index = buscarDistrito_Barrio(distritos, distritos_barrios.get("COD_DISTRITO"), "_id"))<0){
		//				Document dist = new Document("_id", distritos_barrios.get("COD_DISTRITO"))
		//						.append("nombre", distritos_barrios.get("DESC_DISTRITO"));
		//				List<Document> barrios = new ArrayList<>();
		//				barrios.add((new Document()
		//						.append("id_barrio",  distritos_barrios.get("COD_BARRIO"))//VER SI ES ESTE CODIGO O EL DE DIST_BARRIO
		//						.append("nombre", distritos_barrios.get("DESC_BARRIO"))));
		//				dist.append("barrios", barrios);
		//				distritos.add(dist);		
		//			}else{
		//				Document dist = distritos.get(index);
		//				List<Document> barrios = (List<Document>) dist.get("barrios");//VER SI ASI VALE O HAY QUE MACHACAR LISTA EN DIST!!
		//				if(buscarDistrito_Barrio(barrios, distritos_barrios.get("COD_BARRIO"), "id_barrio")<0){
		//				Document barrio = new Document()
		//						.append("id_barrio",  distritos_barrios.get("COD_BARRIO"))//VER SI ES ESTE CODIGO O EL DE DIST_BARRIO
		//						.append("nombre", distritos_barrios.get("DESC_BARRIO"));
		//				barrios.add(barrio);
		//				dist.replace("barrios", (List<Document>) dist.get("barrios"), barrios);
		//				distritos.remove(index);//actualizamos
		//				distritos.add(dist);
		//				}
		//			}
		//			//TERMINAR AÑADIENDO PADRON!! VER ARCHIVO CON SIGNIFICADO ID EDAD ETC
		//		}
		//		distritos_barrios.close();
		System.out.println(distritos.get(0).toJson());
		//collection.insertMany(distritos);//insertamos los distritos
	}

	private Document completarBarrioNuevo(CsvReader distritos_barrios, int i) {
		try{
			Document barrio = new Document()
					.append("id_barrio", "")
					.append("nombre", distritos_barrios.get(i))
					.append("codigo_postal",  new ArrayList<Integer>(){{
						add(Integer.parseInt(buscarValor(distritos_barrios, "codigo postal")));}})
					.append("superfice (m2)", Double.parseDouble(buscarValor(distritos_barrios, "superfice")))
					.append("perimetro (m)", Double.parseDouble(buscarValor(distritos_barrios, "perimetro")));

			List<Document> zonas = new ArrayList<Document>();
			zonas.add(new Document()
					.append("PK", Integer.parseInt(buscarValor(distritos_barrios, "PK")))
					.append("actividad", "Poner con meaningcloud, sobre name archivo")
					.append("sub-actividad", "Poner con meaningcloud, sobre name archivo")
					.append("nombre", buscarValor(distritos_barrios, "nombre"))
					.append("descripcion", buscarValor(distritos_barrios, "descripcion"))
					.append("horario", buscarValor(distritos_barrios, "horario"))
					.append("transporte", buscarValor(distritos_barrios, "transporte"))
					.append("telefono", buscarValor(distritos_barrios, "telefono"))
					.append("email", buscarValor(distritos_barrios, "email"))
					.append("codigo_postal", Integer.parseInt(buscarValor(distritos_barrios, "codigo postal")))
					.append("geo", new Document("type","Point")
							.append("coordinates", new ArrayList<Double>(){{
								add(Double.parseDouble(buscarValor(distritos_barrios, "longitud").replaceAll(",", "")));
								add(Double.parseDouble(buscarValor(distritos_barrios, "latitud").replaceAll(",", "")));}})));
								barrio.append("zonas", zonas);
								return barrio;
		}catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private String buscarValor(CsvReader distritos_barrios, String aprox) {
		try{
			String[] headers = distritos_barrios.getHeaders();
			double max = 0.0;
			double aux = 0.0;
			String header = null;
			for(int i = 0; i<headers.length; i++){
				if( (aux = similarity(headers[i], aprox)) > max && aux > 0.66){
					max = aux;
					header = headers[i];
				}
			}
			if(!distritos_barrios.get(header).equals("")){
				return distritos_barrios.get(header);
			}else{
				return "0";
			}
		}catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private double similarity(String s1, String s2) {
		String longer = s1.toLowerCase(), shorter = s2.toLowerCase();
		if (s1.length() < s2.length()) { // longer should always have greater length
			longer = s2.toLowerCase(); shorter = s1.toLowerCase();
		}
		int longerLength = longer.length();
		if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
		return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) / (double) longerLength;
	}

	/**
	 * Nombre barrio y dist + posicion cabecera
	 * @param distritos_barrios
	 * @return
	 */
	private int[] buscarDistritoBarrioInfo(CsvReader distritos_barrios) {
		int[] dist_barrio_Index = new int[2];
		int cnt = 0;
		try{
			String[] headers = distritos_barrios.getHeaders();
			for(int i = 0; (i<headers.length)&&(cnt<2); i++){
				if(headers[i].toLowerCase().equals("distrito")){
					dist_barrio_Index[0] = i;
					cnt++;
				}else if(headers[i].toLowerCase().equals("barrio")){
					dist_barrio_Index[1] = i;
					cnt++;
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		if(cnt>=2){
			return dist_barrio_Index;
		}
		return null;
	}

	private int buscarDistrito_Barrio(List<Document> distritos_barrios, String code, String id){
		for(Document dist_bar:distritos_barrios){
			if(dist_bar.get(id).toString().equals(code)){
				return distritos_barrios.indexOf(dist_bar);
			}
		}
		return -1;
	}

	public static double similarityBorrar(String s1, String s2) {
		String longer = s1.toLowerCase(), shorter = s2.toLowerCase();
		if (s1.length() < s2.length()) { // longer should always have greater length
			longer = s2.toLowerCase(); shorter = s1.toLowerCase();
		}
		int longerLength = longer.length();
		if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
		return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) / (double) longerLength;
	}

	public static void main(String[] args) throws JSONException, FileNotFoundException, IOException, ParseException  {
		Almacenar alm = new Almacenar();
		alm.generarColeccion();
		//		String a = "02";
		//		System.out.println(Integer.parseInt(a));
		//alm.client.close();
		//				String a = "cod_POSTAL";
		//				String b = "codigo postal";
		//				System.out.println(similarityBorrar(a, b));
		// omp parallel for schedule(dynamic)
		//        for (int i = 2; i < 20; i += 3) {
		//            System.out.println("  @" + i);
		//        }
	}
}
