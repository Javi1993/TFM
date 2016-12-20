package preprocesamiento;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	private HashMap<String, List<String>> ID_datasets;

	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<Document> collection;


	public Almacenar(HashMap<String, List<String>> ID_datasets){
		//		client = new MongoClient("localhost", 27017);//conectamos
		//		database = client.getDatabase("tfm");//elegimos bbdd
		//		collection = database.getCollection("distritos");//tomamos la coleccion de estaciones de aire

		this.ID_datasets = ID_datasets;
		generarColeccion();
	}
	
	public Almacenar(){
		
	}

	/**
	 * @throws IOException 
	 * 
	 */
	private void generarColeccion(){
		//collection.drop();
		try{
			//************************************************************************
			File folder = new File(".\\documents\\PK_FORMAT");
			List<Document> distritos = new ArrayList<Document>();
			for (File fileEntry : folder.listFiles()) {
				if (!fileEntry.isDirectory()) {
					CsvReader distritos_zonas = new CsvReader(fileEntry.getAbsolutePath(),';');
					distritos_zonas.readHeaders();
					int index = 0;
					int[] dist_barrio_index = null;
					while (distritos_zonas.readRecord()){
						//						if(distritos_zonas.get("PK").equals("4672")){
						//							System.out.println(fileEntry.getName());
						//						}
						if( (dist_barrio_index = buscarDistritoBarrioInfo(distritos_zonas)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio
							if(distritos.isEmpty() || (index = buscarDistrito_Barrio(distritos, distritos_zonas.get(dist_barrio_index[0]), "nombre"))<0){//distrito nuevo
								Document dist = new Document().append("nombre", distritos_zonas.get(dist_barrio_index[0]));//cogemos el documento del distrito
								List<Document> barrios = new ArrayList<Document>();//lista de barrios del sitrito
								Document barrio = completarBarrioNuevo(distritos_zonas, dist_barrio_index[1]);//completamos la info del barrio con su zona asociada
								if(!barrio.get("nombre").equals("")){//el formato es correcto, lo añadimos a la lista de barrios
									barrios.add(barrio);
								}
								dist.append("barrios", barrios);
								distritos.add(dist);	
							}else{//el distrito ya esta guardado
								Document dist = distritos.get(index);//cogemos el documento del distrito
								List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
								int index_b = 0;
								if(!barrios.isEmpty() && (index_b = buscarDistrito_Barrio(barrios, distritos_zonas.get(dist_barrio_index[1]), "nombre"))>=0){//barrio ya existente
									Document barrio = barrios.get(index_b);
									((Set<Integer>)barrio.get("codigo_postal")).add(Integer.parseInt(buscarValor(distritos_zonas, "codigo postal", "number")));
									((List<Document>) barrio.get("zonas")).add(addZona(distritos_zonas));
									barrios.remove(index_b);
									barrios.add(barrio);
									//SI EL BARRIO EXISTE SE AÑADE UNA NUEVA ZONA(PK), AUMENTAR CP (FALTA ESTO) y AÑADIR A LISTA NUEVA ZONA(PK)
								}else{//barrio nuevo
									Document barrio = completarBarrioNuevo(distritos_zonas, dist_barrio_index[1]);
									if(!barrio.get("nombre").equals("")){//el formato es correcto, lo añadimos a la lista de barrios
										barrios.add(barrio);
									}
								}
								dist.replace("barrios", (List<Document>) dist.get("barrios"), barrios);
								distritos.remove(index);//actualizamos
								distritos.add(dist);//añade distrito actualizado
							}
						}
					}
				}
			}

			//PK SOLO ESTA PARTE DE ARRIBA, hacer resto!!!!!!!!!!!! ***********************************

			System.out.println(distritos.get(0).toJson());
			//collection.insertMany(distritos);//insertamos los distritos
		}catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Document completarBarrioNuevo(CsvReader distritos_zonas, int i) {
		try{
			Document barrio = new Document()
					.append("id_barrio", "")
					.append("nombre", distritos_zonas.get(i))
					.append("codigo_postal",  new HashSet<Integer>(){{
						add(Integer.parseInt(buscarValor(distritos_zonas, "codigo postal", "number")));}})
					.append("superfice (m2)", Double.parseDouble(buscarValor(distritos_zonas, "superfice", "number")))
					.append("perimetro (m)", Double.parseDouble(buscarValor(distritos_zonas, "perimetro", "number")));

			List<Document> zonas = new ArrayList<Document>();
			zonas.add(addZona(distritos_zonas));
			barrio.append("zonas", zonas);
			return barrio;
		}catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private Document addZona(CsvReader distritos_zonas) {
		Document zonaPK = new Document()
				.append("PK", Integer.parseInt(buscarValor(distritos_zonas, "PK", "number")))
				.append("actividad", bucarTopic())
				.append("sub-actividad", "")
				.append("nombre", buscarValor(distritos_zonas, "nombre", "text"))
				.append("descripcion", buscarValor(distritos_zonas, "descripcion", "text"))
				.append("horario", buscarValor(distritos_zonas, "horario", "text"))
				.append("transporte", buscarValor(distritos_zonas, "transporte", "text"))
				.append("telefono", buscarValor(distritos_zonas, "telefono", "text"))
				.append("email", buscarValor(distritos_zonas, "email", "text"))
				.append("codigo_postal", Integer.parseInt(buscarValor(distritos_zonas, "codigo postal", "number")))
				.append("geo", new Document("type","Point")
						.append("coordinates", new ArrayList<Double>(){{
							add(Double.parseDouble(buscarValor(distritos_zonas, "longitud", "text").replaceAll(",", "")));
							add(Double.parseDouble(buscarValor(distritos_zonas, "latitud", "text").replaceAll(",", "")));}}));
		return limpiarDoc(zonaPK);
	}

	private String bucarTopic() {
		
		return null;
	}

	private Document limpiarDoc(Document doc) {
		Document aux = new Document(doc);
		for(String label:doc.keySet()){
			String value = doc.get(label).toString();
			if(value.equals("")||value.equals("0")){
				aux.remove(label);
			}
		}
		return aux;
	}

	private String buscarValor(CsvReader distritos_zonas, String aprox, String tipo) {
		try{
			String[] headers = distritos_zonas.getHeaders();
			double max = 0.0;
			double aux = 0.0;
			String header = null;
			for(int i = 0; i<headers.length; i++){
				if( (aux = similarity(headers[i], aprox)) > max && aux > 0.66){
					max = aux;
					header = headers[i];
				}
			}
			String value = distritos_zonas.get(header);
			if(!value.equals("")){
				if(tipo.equals("number")){//cogemos solo la parte numerica
					value = value.replaceAll("\\s+","");
					Pattern p = Pattern.compile("(\\d+)");
					Matcher m = p.matcher(value);
					if (m.find()) {
						return m.group(1);
					}
				}
				return value.trim();
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

		//		Pattern p = Pattern.compile("(\\d+)");
		//		Matcher m = p.matcher("SN - 28040");
		//		Integer j = null;
		//		if (m.find()) {
		//			j = Integer.valueOf(m.group(1));
		//		}
		//		System.out.println(j);

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
