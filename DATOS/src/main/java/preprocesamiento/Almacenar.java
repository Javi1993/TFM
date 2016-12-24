package preprocesamiento;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.json.JSONException;
import org.json.simple.parser.ParseException;
import com.csvreader.CsvReader;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import gov.nasa.worldwind.avlist.AVKey;
import gov.nasa.worldwind.geom.LatLon;
import gov.nasa.worldwind.geom.coords.UTMCoord;
import preprocesamiento.meaningcloud.ClassClient;

@SuppressWarnings({"serial", "unchecked"})
public class Almacenar {

	private HashMap<String, String> dataset_ID;

	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<Document> collection;


	public Almacenar(HashMap<String, String> dataset_ID){
		this.dataset_ID = dataset_ID;
		cargarDatos();
	}

	private void conDB(){
		client = new MongoClient("localhost", 27017);//conectamos
		database = client.getDatabase("tfm");//elegimos bbdd
		collection = database.getCollection("distritos");//tomamos la coleccion de estaciones de aire
	}

	private void cargarDatos(){
		List<Document> distritos = generarDistritosBarrios();//generamos los 21 distritos y sus barrios en base al padron
		if(!distritos.isEmpty()&&distritos.size()==21){
			System.out.println("Estructura basica de distritos y barrios creada.");
			distritos = generarZonas(distritos);//formato PK
			System.out.println("Insertadas las zonas con formato PK.");
			generarDistritoFormat(distritos);
		}else{
			System.out.println("La carga inicial de distritos y barrios es erronea.");
		}
	}

	/**
	 * SIMPLIFICAR!! CADA SWITCH EN UNA UNICA FUNCION LO COMUN!! VER NOTEPADD++ CON MAS NOTAS
	 * @param distritos
	 */
	private void generarDistritoFormat(List<Document> distritos) {
		try{
			conDB();
			collection.drop();
			client.close();
			File folder = new File(".\\documents\\DISTRICT_FORMAT");
			for (File fileEntry : folder.listFiles()){ 
				if (!fileEntry.isDirectory()) {
					CsvReader distritos_locs = new CsvReader(fileEntry.getAbsolutePath(),';');
					distritos_locs.readHeaders();
					String[] names = {"animal", "ropa", "pila", "fuente", "deportes"};
					int i;
					for(i = 0; i < names.length; i++){
						if(fileEntry.getName().contains(names[i])) break;
					}
					switch(i) {
					case 0:
						distritos = addDistritoLoc(distritos, distritos_locs, "censo_animales_domesticos");
						break;
					case 1:
					case 2:
						//							if(i==1){//ropa CAMBIAR SEGUN TIPO!!
						//								distritos = addDistritoLoc(distritos, dist_index, distritos_locs, index, "contenedores");
						//							}else{//pila
						//								distritos = addDistritoLoc(distritos, dist_index, distritos_locs, index, "contenedores");
						//							}
						break;
					case 3:
						distritos = addDistritoLoc(distritos, distritos_locs, "fuentes_potables");
						break;
					case 4:
						distritos = addDistritoLoc(distritos, distritos_locs, "actividades_deportivas");
						break;
					default:
						System.out.println("No hay desarrolo para preprocesar "+fileEntry.getName());
					}

				}
			}
			conDB();
			collection.insertMany(distritos);//insertamos los distritos
			client.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	//CAMBIAR NOMBRE VARIANBLES (ANIUMALES) PARA DEJAR DE FORMA GENERAL
	private List<Document> addDistritoLoc(List<Document> distritos, CsvReader distritos_locs, String document) throws NumberFormatException, IOException{
		List<String> attrList = getCampos(document, 0);
		String attr;
		int index = 0;
		int[] dist_index = null;
		if( (dist_index = buscarDistritoBarrioInfo(distritos_locs, 1)) !=null ){//obtenemos la posicion de la cabeceras distrito en el CSV		
			while (distritos_locs.readRecord()){
				Document doc = new Document();
				index = buscarDistrito_Barrio_Zona(distritos, distritos_locs.get(dist_index[0]).trim(), "nombre");//obtenemos la posicion en la lista del doc del distrito
				if(index>=0){
					Document dist = distritos.get(index);//cogemos el documento del distrito
					for(String label:attrList){
						if((attr = buscarValor(distritos_locs, label.split("&&")[0], label.split("&&")[1]))!=null && !label.split("&&")[0].equals("geo")){
							if(StringUtils.isNumeric(label.split("&&")[1])){
								doc.append(label.split("&&")[0], Double.parseDouble(attr));
							}else{
								doc.append(label.split("&&")[0], attr);
							}
						}else{
							String lat, lon, east, nort;
							if((lat = buscarValor(distritos_locs, "latitud", "text"))!=null
									&& (lon = buscarValor(distritos_locs, "longitud", "text"))!=null){
								doc.append("geo", new Document("type","Point")
										.append("coordinates", new ArrayList<Double>(){{
											add(Double.parseDouble(lon.replaceAll(",", "")));
											add(Double.parseDouble(lat.replaceAll(",", "")));
										}}));
							}else if((east = buscarValor(distritos_locs, "coord X", "text"))!=null
									&& (nort = buscarValor(distritos_locs, "coord Y", "text"))!=null){
								LatLon coordinates = UTMCoord.locationFromUTMCoord(30, AVKey.NORTH, Double.parseDouble(east.replaceAll(",", ".")), Double.parseDouble(nort.replaceAll(",", ".")));
								doc.append("geo", new Document("type","Point")
										.append("coordinates", new ArrayList<Double>(){{
											add(coordinates.getLongitude().getDegrees());
											add(coordinates.getLatitude().getDegrees());
										}}));
							}
						}
					}
					List<Document> documents =  (List<Document>) dist.get(document);
					if(documents!=null){
						documents.add(doc);
						dist.replace(document, documents);
					}else{
						dist.append(document, new ArrayList<Document>(){{
							add(doc);}});
					}
					distritos.remove(index);
					distritos.add(dist);
				}
			}
		}
		return distritos;
	}

	private List<String> getCampos(String partOfJSON, int nivel){
		List<String> campos = new ArrayList<String>();
		try {
			byte[] encoded = Files.readAllBytes(Paths.get("./extras/JSON_example_TFM.json"));
			Document JSON = Document.parse(new String(encoded, "ISO-8859-1"));
			if(nivel > 0){//bajamos a subnivel en JSON
				JSON = ((List<Document>) JSON.get("barrios")).get(0);
			}
			List<Document> docs = (List<Document>)JSON.get(partOfJSON);
			Document doc = docs.get(0);
			for(String label:doc.keySet()){
				campos.add(label+"&&"+doc.get(label));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return campos;
	}

	private List<Document> generarDistritosBarrios(){
		//		collection.drop();
		File dir = new File("./documents/DISTRICT_BARRIO_FORMAT/");
		FileFilter fileFilter = new WildcardFileFilter("*padron.csv");
		try{
			CsvReader distritos_barrios = new CsvReader (dir.listFiles(fileFilter)[0].getAbsolutePath(), ';');
			distritos_barrios.readHeaders();
			List<Document> distritos = new ArrayList<>();
			List<String> attrPadron = getCampos("padron", 1);
			int[] dist_barrio_index = null;
			while (distritos_barrios.readRecord()){//recorremos el CSV
				int index = 0;//posicion en la lista del distrito
				if( (dist_barrio_index = buscarDistritoBarrioInfo(distritos_barrios, 2)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio
					if(distritos.isEmpty() || (index = buscarDistrito_Barrio_Zona(distritos, distritos_barrios.get(dist_barrio_index[0]).trim(), "nombre"))<0){//distrito nuevo
						Document dist = new Document("_id", distritos_barrios.get("COD_DISTRITO")).append("nombre", distritos_barrios.get(dist_barrio_index[0]).trim());//cogemos el documento del distrito
						List<Document> barrios = new ArrayList<Document>();//lista de barrios del sitrito
						Document bar = new Document("_id", distritos_barrios.get("COD_BARRIO")).append("nombre", distritos_barrios.get(dist_barrio_index[1]).trim());
						if(!bar.get("nombre").equals("")&&!bar.get("_id").equals("")){//el formato es correcto, lo a�adimos a la lista de barrios
							List<Document> padron = new ArrayList<Document>();
							padron.add(addNewAgePadron(distritos_barrios, attrPadron));
							bar.append("padron", padron);
							barrios.add(bar);
						}
						dist.append("barrios", barrios);
						distritos.add(dist);	
					}else{//ya existe distrito, a�adimos barrio
						Document dist = distritos.get(index);//cogemos el documento del distrito
						List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
						int index_b = 0;//posicion en la lista del barrio						
						if(!barrios.isEmpty() && (index_b = buscarDistrito_Barrio_Zona(barrios, distritos_barrios.get(dist_barrio_index[1]).trim(), "nombre"))>=0){//ya contiene ese barrio
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
							//a�adir si no se actualizo, meter boolean o algo
						}else{//no tiene el barrio
							Document bar = new Document("_id", distritos_barrios.get("COD_BARRIO")).append("nombre", distritos_barrios.get(dist_barrio_index[1]).trim());
							if(!bar.get("nombre").equals("")&&!bar.get("_id").equals("")){//el formato es correcto, lo a�adimos a la lista de barrios
								List<Document> padron = new ArrayList<Document>();
								padron.add(addNewAgePadron(distritos_barrios, attrPadron));
								bar.append("padron", padron);
								barrios.add(bar);
							}
							//							dist.append("barrios", barrios);
							dist.replace("barrios", barrios);
							distritos.remove(index);//actualizamos
							distritos.add(dist);//a�ade distrito actualizado
						}
					}
				}
			}
			//			System.out.println(distritos.size());
			//			System.out.println(distritos.get(0).toJson());
			//			for(Document d:distritos){
			//				System.out.println(d.get("_id")+"__"+d.get("nombre"));
			//			}
			//			collection.insertMany(distritos);//insertamos los distritos
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
	 * 
	 * @param distritos
	 * @return
	 */
	private List<Document> generarZonas(List<Document> distritos){
		try{

			File folder = new File(".\\documents\\PK_FORMAT");
			for (File fileEntry : folder.listFiles()) {
				if (!fileEntry.isDirectory()) {
					CsvReader distritos_zonas = new CsvReader(fileEntry.getAbsolutePath(),';');
					distritos_zonas.readHeaders();
					int index = 0;//posicion en la lista del distrito
					int[] dist_barrio_index = null;
					Set<String> topics = getRol(fileEntry.getName());
					while (distritos_zonas.readRecord()){
						if( (dist_barrio_index = buscarDistritoBarrioInfo(distritos_zonas, 2)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio en el CSV
							index = buscarDistrito_Barrio_Zona(distritos, distritos_zonas.get(dist_barrio_index[0]).trim(), "nombre");//obtenemos la posicion en la lista del doc del distrito
							//							System.out.println("INDEX D "+index);
							if(index>=0){//LOCALIZAR POR COORDENADAS SI NO TIENE BARRIO O DISTTRITO EN EL CSV!!
								Document dist = distritos.get(index);//cogemos el documento del distrito
								List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
								int index_b = buscarDistrito_Barrio_Zona(barrios, distritos_zonas.get(dist_barrio_index[1]).trim(), "nombre");//posicion en la lista del barrio
								//								System.out.println("INDEX B "+index_b);
								if(index_b>=0){
									Document barrio = barrios.get(index_b);//cogemos el documento del barrio
									String cod_postal = buscarValor(distritos_zonas, "codigo_postal", "1");
									if(cod_postal!=null && !cod_postal.equals("")){
										if(barrio.get("codigo_postal")!=null&&!barrio.get("codigo_postal").equals("")){
											//										System.out.println(fileEntry.getName()+"_"+buscarValor(distritos_zonas, "PK", "1")+"_"+buscarValor(distritos_zonas, "codigo_postal", "1"));
											((Set<Integer>)barrio.get("codigo_postal")).add(Integer.parseInt(cod_postal));
										}else{
											barrio.append("codigo_postal",  new HashSet<Integer>(){{
												add(Integer.parseInt(cod_postal));}});
										}
									}
									if(barrio.get("zonas")!=null&&!barrio.get("zonas").equals("")){//ya hay zonas guardadas para ese barrio
										//comprobar si zona PK ya existe (buscar por PK) y fusiinar topics
										int index_z;
										if((index_z = buscarDistrito_Barrio_Zona((List<Document>) barrio.get("zonas"), distritos_zonas.get("PK"), "PK")) == -1){
											((List<Document>) barrio.get("zonas")).add(addZona(distritos_zonas, topics));
										}else{
											if(topics!=null && !topics.isEmpty()){
												System.out.println("ENTRO "+fileEntry.getName());
												for(String top:topics){
													((Set<String>)((Document)((List<Document>) barrio.get("zonas")).get(index_z)).get("rol")).add(top);
												}
											}	
										}
									}else{
										barrio.append("zonas", new ArrayList<Document>(){{
											add(addZona(distritos_zonas, topics));
										}});
									}
									barrios.remove(index_b);
									barrios.add(barrio);	
									dist.replace("barrios", barrios);	
									distritos.remove(index);//actualizamos
									distritos.add(dist);//a�ade distrito actualizado	
								}	
							}
						}
					}
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		return distritos;
	}

	private Set<String> getRol(String nameFile){
		ClassClient mc = new ClassClient();
		Set<String> topic = mc.tematicaDataset(dataset_ID.get(nameFile));
		return topic;
	}

	/**
	 * 
	 * @param distritos_zonas
	 * @param topics
	 * @return
	 */
	private Document addZona(CsvReader distritos_zonas, Set<String> topics) {
		List<String> attrZonas = getCampos("zonas", 1);
		Document zona = new Document();
		String attr = null;
		for(String label:attrZonas){
			attr = buscarValor(distritos_zonas, label.split("&&")[0], label.split("&&")[1]);
			if(attr!=null&&!label.split("&&")[0].equals("geo")&&!label.split("&&")[0].equals("rol")){
				if(StringUtils.isNumeric(label.split("&&")[1])){
					zona.append(label.split("&&")[0], Double.parseDouble(attr));
				}else{
					zona.append(label.split("&&")[0], attr);
				}
			}else{
				if(label.split("&&")[0].equals("rol")&&!topics.isEmpty()){
					zona.append("rol", topics);
				}else if(label.split("&&")[0].equals("geo")){
					String lat, lon;
					if((lat = buscarValor(distritos_zonas, "latitud", "text"))!=null
							&& (lon = buscarValor(distritos_zonas, "longitud", "text"))!=null){
						zona.append("geo", new Document("type","Point")
								.append("coordinates", new ArrayList<Double>(){{
									add(Double.parseDouble(lon.replaceAll(",", "")));
									add(Double.parseDouble(lat.replaceAll(",", "")));
								}}));
					}
				}
			}
		}
		return zona;
	}

	//	private Document limpiarDoc(Document doc) {
	//		Document aux = new Document(doc);
	//		for(String label:doc.keySet()){
	//			String value = doc.get(label).toString();
	//			if(value.equals("")||value.equals("0")){
	//				aux.remove(label);
	//			}
	//		}
	//		return aux;
	//	}

	private String buscarValor(CsvReader distritos_zonas, String aprox, String tipo) {
		try{
			String[] headers = distritos_zonas.getHeaders();
			double max = 0.0;
			double aux = 0.0;
			String header = null;
			for(int i = 0; i<headers.length; i++){
				if( (aux = similarity(headers[i], aprox)) > max && aux > 0.55){
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
	private int[] buscarDistritoBarrioInfo(CsvReader distritos_barrios, int cnt_main) {
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
		if(cnt==cnt_main){
			return dist_barrio_Index;
		}
		return null;
	}

	/**
	 * Dada una lista de distritos/barrios/zonas devulve la posicion que ocupa en ella
	 * @param distritos_barrios: lista de distritos, barrios o zonas
	 * @param code: valor del campo
	 * @param id: nombre del campo
	 * @return: Posicion en la lista
	 */
	private int buscarDistrito_Barrio_Zona(List<Document> distritos_barrios_zonas, String code, String id){
		for(Document dist_bar:distritos_barrios_zonas){
			if(dist_bar.get(id).equals(code)||dist_bar.get(id).toString().split("-")[0].equals(code)
					||(!id.equals("PK")&&similarity(dist_bar.get(id).toString(), code)>0.8)){
				return distritos_barrios_zonas.indexOf(dist_bar);
			}
		}
		return -1;
	}

	static double similarityBorrar(String s1, String s2) {
		String longer = s1.toLowerCase(), shorter = s2.toLowerCase();
		if (s1.length() < s2.length()) { // longer should always have greater length
			longer = s2.toLowerCase(); shorter = s1.toLowerCase();
		}
		int longerLength = longer.length();
		if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
		return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) / (double) longerLength;
	}

	public static void main(String[] args) throws JSONException, FileNotFoundException, IOException, ParseException  {
		//						Almacenar alm = new Almacenar(null);
		//	alm.generarZonas(null);
		//		alm.generarDistritosBarrios();

		//		Pattern p = Pattern.compile("(\\d+)");
		//		Matcher m = p.matcher("SN - 28040");
		//		Integer j = null;
		//		if (m.find()) {
		//			j = Integer.valueOf(m.group(1));
		//		}
		//		System.out.println(j);
//		String a = "40.36581776978113";
//		System.out.println();
		//		String a = "02";
		//		System.out.println(Integer.parseInt(a));
		//		alm.client.close();
		//				String a = "COORD.X";
		//				String b = "coord X";
		//				System.out.println(similarityBorrar(a, b));
		// omp parallel for schedule(dynamic)
		//        for (int i = 2; i < 20; i += 3) {
		//            System.out.println("  @" + i);
		//        }
	}
}
