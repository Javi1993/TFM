package preprocesamiento;


import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.apache.commons.io.filefilter.WildcardFileFilter;
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
				client = new MongoClient("localhost", 27017);//conectamos
				database = client.getDatabase("tfm");//elegimos bbdd
				collection = database.getCollection("distritos");//tomamos la coleccion de estaciones de aire

		this.ID_datasets = ID_datasets;
		generarDistritosBarrios();
	}

	public Almacenar(){

	}

	private List<String> getCamposBarrios(String partOfJSON){
		List<String> campos = new ArrayList<String>();
		try {
			byte[] encoded = Files.readAllBytes(Paths.get("./extras/JSON_example_TFM.json"));
			Document JSON = new Document().parse(new String(encoded, "ISO-8859-1"));
			
			//modificar para acceder a la lista en ujna sola linea saltando niveles
			List<Document> docs = (List<Document>) JSON.get("barrios");//cambiar
			List<Document> docsA = (List<Document>) docs.get(0).get(partOfJSON);
			Document doc = docsA.get(0);
			for(String label:doc.keySet()){
				campos.add(label+"&&"+doc.get(label));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return campos;
	}
	
	private List<Document> generarDistritosBarrios(){
		collection.drop();
		File dir = new File("./documents/DISTRICT_BARRIO_FORMAT/");
		FileFilter fileFilter = new WildcardFileFilter("*padron.csv");
		try{
			CsvReader distritos_barrios = new CsvReader (dir.listFiles(fileFilter)[0].getAbsolutePath(), ';');
			distritos_barrios.readHeaders();
			List<Document> distritos = new ArrayList<>();
			List<String> attrPadron = getCamposBarrios("padron");
//			List<String> attrPadron = new ArrayList<String>(){{ //LEER DE JSON!! COGER FICHERO E IR A PADRON Y COGER SUS VALORES
//				add("cod_edad&&number");
//				add("n_españoles_hombres&&number");
//				add("n_extranjeros_hombres&&number");
//				add("n_españoles_mujeres&&number");
//				add("n_extranjeros_mujeres&&number");//cambiar separador
//			}};
			int[] dist_barrio_index = null;
			while (distritos_barrios.readRecord()){//recorremos el CSV
				int index = 0;//posicion en la lista del distrito
				if( (dist_barrio_index = buscarDistritoBarrioInfo(distritos_barrios)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio
					if(distritos.isEmpty() || (index = buscarDistrito_Barrio(distritos, distritos_barrios.get(dist_barrio_index[0]).trim(), "nombre"))<0){//distrito nuevo
						Document dist = new Document("_id", distritos_barrios.get("COD_DISTRITO")).append("nombre", distritos_barrios.get(dist_barrio_index[0]).trim());//cogemos el documento del distrito
						List<Document> barrios = new ArrayList<Document>();//lista de barrios del sitrito
						Document bar = new Document("_id", distritos_barrios.get("COD_BARRIO")).append("nombre", distritos_barrios.get(dist_barrio_index[1]).trim());
						if(!bar.get("nombre").equals("")&&!bar.get("_id").equals("")){//el formato es correcto, lo añadimos a la lista de barrios
							List<Document> padron = new ArrayList<Document>();
							padron.add(addNewAgePadron(distritos_barrios, attrPadron));
							bar.append("padron", padron);
							barrios.add(bar);
						}
						dist.append("barrios", barrios);
						distritos.add(dist);	
					}else{//ya existe distrito, añadimos barrio
						Document dist = distritos.get(index);//cogemos el documento del distrito
						List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
						int index_b = 0;//posicion en la lista del barrio						
						if(!barrios.isEmpty() && (index_b = buscarDistrito_Barrio(barrios, distritos_barrios.get(dist_barrio_index[1]).trim(), "nombre"))>=0){//ya contiene ese barrio
							Document bar = barrios.get(index_b);
							List<Document> padron = (List<Document>) bar.get("padron");
							if(padron!=null){
								boolean cambiado = false;
								for(Document pad:padron){
									if(pad.getDouble("cod_edad")==Double.parseDouble((buscarValor(distritos_barrios, "cod_edad", "0")))){
										//actualizar padron
										for(String label:attrPadron){
											if(!label.split("&&")[0].equals("cod_edad")){
												String valor;
												if((valor = buscarValor(distritos_barrios, label.split("&&")[0], label.split("&&")[1]))!=null){
													if(pad.get(label.split("&&")[0])!=null){
														pad.replace(label.split("&&")[0], pad.getDouble(label.split("&&")[0])+Double.valueOf(valor));
													}else{//todavia no se tomo valores para ese tipo
														pad.append(label.split("&&")[0], Double.valueOf(valor));
													}
												}
											}
										}
										cambiado=true;
										break;
									}
								}
								if(!cambiado){
									padron.add(addNewAgePadron(distritos_barrios, attrPadron));
								}
							}
							bar.replace("padron", padron);
							barrios.remove(index_b);
							barrios.add(bar);
							dist.replace("barrios", barrios);
							distritos.remove(index);
							distritos.add(dist);
							//añadir si no se actualizo, meter boolean o algo
						}else{//no tiene el barrio
							Document bar = new Document("_id", distritos_barrios.get("COD_BARRIO")).append("nombre", distritos_barrios.get(dist_barrio_index[1]).trim());
							if(!bar.get("nombre").equals("")&&!bar.get("_id").equals("")){//el formato es correcto, lo añadimos a la lista de barrios
								List<Document> padron = new ArrayList<Document>();
								padron.add(addNewAgePadron(distritos_barrios, attrPadron));
								bar.append("padron", padron);
								barrios.add(bar);
							}
							//							dist.append("barrios", barrios);
							dist.replace("barrios", barrios);
							distritos.remove(index);//actualizamos
							distritos.add(dist);//añade distrito actualizado
						}
					}
				}
			}
			System.out.println(distritos.size());
			System.out.println(distritos.get(0).toJson());
			collection.insertMany(distritos);//insertamos los distritos
			return distritos;
		}catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Genera un Document para un cod_edad del padron de un barrio
	 * @param distritos_barrios - CsV con datos
	 * @param attrPadron - Atributos a buscar
	 * @return Document con los datos
	 */
	private Document addNewAgePadron(CsvReader distritos_barrios, List<String> attrPadron) {
		Document pad = new Document();
		String attr;
		for(String label:attrPadron){
			if((attr = buscarValor(distritos_barrios, label.split("&&")[0], label.split("&&")[1]))!=null){
				if(StringUtils.isNumeric(label.split("&&")[1])){
					pad.append(label.split("&&")[0], Double.parseDouble(attr));
				}else{
					pad.append(label.split("&&")[0], attr);
				}
			}
		}
		return pad;
	}

	/**
	 * @throws IOException 
	 * 
	 */
	private void generarColeccion(){
		//collection.drop();
		
		//CAMBIAR!! TODOS LOS DISTRITOS Y BARRIOS YA EXISTEN!! SOLO
		//INSERTAR LOS PK EN EL BARRIO CORRESPONDIENTE
		
		
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
				if( (aux = similarity(headers[i], aprox)) > max && aux > 0.60){
					max = aux;
					header = headers[i];
				}
			}
			String value = distritos_zonas.get(header);
			if(!value.equals("")){
				if(StringUtils.isNumeric(tipo)){//cogemos solo la parte numerica
					value = value.replaceAll("\\s+","");
					Pattern p = Pattern.compile("(\\d+)");
					Matcher m = p.matcher(value);
					if (m.find()) {
						return m.group(1);
					}
				}
				return value.trim();
			}else{
				return null;
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
				if(headers[i].toLowerCase().equals("distrito")||headers[i].toLowerCase().equals("desc_distrito")){
					dist_barrio_Index[0] = i;
					cnt++;
				}else if(headers[i].toLowerCase().equals("barrio")||headers[i].toLowerCase().equals("desc_barrio")){
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
			if(dist_bar.get(id).equals(code)){
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
		Almacenar alm = new Almacenar(null);
		//		alm.generarColeccion();
//		alm.generarDistritosBarrios();

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
		//						String a = "n_españoles_hombres";
		//						String b = "EspanolesHombres";
		//						System.out.println(similarityBorrar(a, b));
		// omp parallel for schedule(dynamic)
		//        for (int i = 2; i < 20; i += 3) {
		//            System.out.println("  @" + i);
		//        }
	}
}
