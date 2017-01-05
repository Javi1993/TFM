package preprocesamiento;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.Document;
import com.csvreader.CsvReader;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import funciones.Funciones;
import gov.nasa.worldwind.avlist.AVKey;
import gov.nasa.worldwind.geom.LatLon;
import gov.nasa.worldwind.geom.coords.UTMCoord;
import preprocesamiento.geocoding.Geocode;
import preprocesamiento.meaningcloud.ClassClient;

@SuppressWarnings({"serial", "unchecked", "deprecation"})
public class Almacenar {

	private HashMap<String, String> dataset_ID;
	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<Document> collection;
	private Geocode geo;
	private SimpleDateFormat formatoFecha;

	public Almacenar(HashMap<String, String> dataset_ID){
		this.dataset_ID = dataset_ID;
		geo = new Geocode();
		cargarDatos();
	}

	private void conDB(){
		client = new MongoClient("localhost", 27017);//conectamos
		database = client.getDatabase("tfm");//elegimos bbdd
		collection = database.getCollection("distritos");//tomamos la coleccion de estaciones de aire
	}

	/**
	 * Procesa todos los ficheros descargados y los almacena en la base de datos
	 */
	private void cargarDatos(){
		List<Document> distritos = null;
		conDB();
		if(collection.count()==0){//la coleccion esta vacia
			client.close();
			distritos = generarDistritosBarrios();
			//generamos los 21 distritos y sus barrios en base al padron
			if(distritos!= null && !distritos.isEmpty() && distritos.size()==21){
				System.out.println("Estructura basica de distritos y barrios creada.");
				lanzarCargas(distritos);//lanzamos las cargas
			}else{
				System.out.println("La carga inicial de distritos y barrios es erronea.");
			}
		}else{//ya existe una coleccion
			FindIterable<Document> list = collection.find();
			MongoCursor<Document> cursor = list.iterator();
			distritos = new ArrayList<Document>();
			while (cursor.hasNext()) {
				distritos.add(cursor.next());
			}
			client.close();
			lanzarCargas(distritos);//lanzamos las cargas
		}
	}

	/**
	 * Lanza el procesamiento y almacenamiento de datos en base al formato del fichero
	 * @param distritos - coleccion de distritos
	 */
	private void lanzarCargas(List<Document> distritos){
		generarZonas(distritos);//formato PK
		generarCatastro(distritos);
		generarDistritoFormat(distritos);
		generarEstaciones(distritos);
		guardarElecciones("*elecciones-ayuntamiento-madrid.*", distritos);
		generarMultas(distritos);
		generarRadares(distritos);
		generarZonaSER(distritos);
		conDB();
		collection.drop();
		collection.insertMany(distritos);//insertamos los distritos con su informacion
		client.close();
	}

	private void generarZonaSER(List<Document> distritos) {
		try {
			File dir = new File("./documents/DISTRICT_BARRIO_FORMAT/");
			FileFilter fileFilter = new WildcardFileFilter("*aparcamientos-SER.*");
			if(dir.exists() && dir.listFiles(fileFilter).length>0){
				for(int i = 0; i<dir.listFiles(fileFilter).length; i++){			
					File auxFile = dir.listFiles(fileFilter)[i];
					if(auxFile.exists()){
						CsvReader ser = new CsvReader(auxFile.getAbsolutePath(),';');
						ser.readHeaders();
						if(ser.getHeaderCount()==5 || ser.getHeaderCount()==8){
							int index = 0;//posicion en la lista del distrito
							int[] dist_barrio_index = null;
							while (ser.readRecord()){
								if( (dist_barrio_index = buscarDistritoBarrioInfo(ser, 2)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio en el CSV
									if(ser.getHeaderCount()==5){
										index = buscarDistrito_Barrio_Zona(distritos, ser.get(dist_barrio_index[0]).split("\\s+",2)[1].trim(), "_id");//obtenemos la posicion en la lista del doc del distrito
									}else{
										index = buscarDistrito_Barrio_Zona(distritos, ser.get(dist_barrio_index[0]).split("\\s+",2)[1].trim(), "nombre");//obtenemos la posicion en la lista del doc del distrito
									}
									if(index>=0){//LOCALIZAR POR COORDENADAS SI NO TIENE BARRIO O DISTTRITO EN EL CSV!!
										Document dist = distritos.get(index);//cogemos el documento del distrito
										List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
										int index_b = buscarDistrito_Barrio_Zona(barrios, ser.get(dist_barrio_index[1]).split("\\s+",2)[1].trim(), "nombre");//obtenemos la posicion en la lista del doc del barrio
										if(index_b>=0){
											Document barrio = barrios.get(index_b);//cogemos el documento del barrio
											List<String> attrZonas = null;
											if(ser.getHeaderCount() == 5){
												attrZonas = getCampos("plazas", "barrios", "zona_ser");
											}else{
												attrZonas = getCampos("parquimetros", "barrios", "zona_ser");
											}
											Document serDoc = new Document();
											String attr = null;
											String east, nort;
											for(String label:attrZonas){
												if((attr = buscarValor(ser, label.split("&&")[0], label.split("&&")[1]))!=null && !label.split("&&")[0].equals("geo")){
													if(NumberUtils.isNumber(label.split("&&")[1])){
														serDoc.append(label.split("&&")[0], Integer.parseInt(attr));
													}else{
														serDoc.append(label.split("&&")[0], attr);
													}
												}else if(label.split("&&")[0].equals("geo") && (east = buscarValor(ser, "gis_x", "text"))!=null
														&& (nort = buscarValor(ser, "gis_y", "text"))!=null){
													LatLon coordinates = UTMCoord.locationFromUTMCoord(30, AVKey.NORTH, Double.parseDouble(east.replaceAll(",", ".")), Double.parseDouble(nort.replaceAll(",", ".")));
													Funciones.setCoordinates(serDoc, coordinates.getLatitude().getDegrees(), coordinates.getLongitude().getDegrees());
												}
											}
											Document zona_ser = (Document) barrio.get("zona_ser");
											if(zona_ser!=null){
												List<Document> listAux = null;
												if(ser.getHeaderCount()==5){
													listAux = (List<Document>) zona_ser.get("plazas");
													if(listAux!=null){
														listAux.add(serDoc);
														zona_ser.replace("plazas", listAux);
													}else{
														zona_ser.append("plazas", new ArrayList<Document>(){{add(serDoc);}});
													}
												}else{
													listAux = (List<Document>) zona_ser.get("parquimetros");
													if(listAux!=null){
														listAux.add(serDoc);
														zona_ser.replace("parquimetros", listAux);
													}else{
														zona_ser.append("parquimetros", new ArrayList<Document>(){{add(serDoc);}});
													}
												}
												barrio.replace("zona_ser", zona_ser);
											}else{
												if(ser.getHeaderCount()==5){
													barrio.append("zona_ser", new Document("plazas",  new ArrayList<Document>(){{add(serDoc);}}));
												}else{
													barrio.append("zona_ser", new Document("parquimetros",  new ArrayList<Document>(){{add(serDoc);}}));
												}
											}
											barrios.remove(index_b);
											barrios.add(barrio);	
											dist.replace("barrios", barrios);	
											distritos.remove(index);//actualizamos
											distritos.add(dist);//añade distrito actualizado	
										}
									}
								}

							}
						}
						ser.close();
					}
				}
				for(File f:dir.listFiles(fileFilter)){	
					Funciones.deleteFile(f);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void generarRadares(List<Document> distritos) {
		try {
			File dir = new File("./documents/UNKNOW_FORMAT/");
			FileFilter fileFilter = new WildcardFileFilter("*300049-0-radares-fijos-moviles*");
			if(dir.exists() && dir.listFiles(fileFilter).length > 0){
				File auxFile = dir.listFiles(fileFilter)[0];
				if(auxFile.exists()){
					CsvReader radares = new CsvReader(auxFile.getAbsolutePath(),';');
					radares.readHeaders();
					while (radares.readRecord()){//recorremos el CSV
						String lon, lat;
						double lonDouble = 0;
						double latDouble = 0;
						lon = "-"+buscarValor(radares, "longitud", "1");
						lat = buscarValor(radares, "latitud", "1");
						String CP = null;
						if(lat != null && lon != null && !lon.equals("") && !lat.equals("")){
							lonDouble = Double.parseDouble(lon.replaceAll("\\s+", ""));
							latDouble = Double.parseDouble(lat.replaceAll("\\s+", ""));
							CP = geo.getCPbyCoordinates(lonDouble, latDouble);
						}
						if(CP==null){//buscamos por calle si no hay coordenadas o no hubo resultado para las dadas
							String ubicacion = buscarValor(radares, "ubicacion", "text");
							if(ubicacion.matches(".*\\(.*\\)$")){
								ubicacion = ubicacion.substring(ubicacion.indexOf('(')+1, ubicacion.lastIndexOf(')')).split("-")[0];
							}else{
								ubicacion = ubicacion.split(",")[0];
							}
							CP = geo.getCPbyStreet(StringUtils.stripAccents(ubicacion));
						}
						if(CP != null && Funciones.checkCP(CP)){
							int index = getDistritoByCP(distritos, CP);//devuelve la posicion que ocupa el distrito con ese CP
							if(index>=0){
								Document dist = distritos.get(index);
								Document radar = new Document();//documento donde se guardara la info de la estacion
								String attr;
								List<String> attrList = getCampos("radares", null, null);
								for(String label:attrList){
									if((attr = buscarValor(radares, label.split("&&")[0], label.split("&&")[1]))!=null && !label.split("&&")[0].equals("geo")){
										if(NumberUtils.isNumber(label.split("&&")[1])){
											radar.append(label.split("&&")[0], Integer.parseInt(attr));
										}else{
											radar.append(label.split("&&")[0], attr);
										}
									}else if(lonDouble != 0 && latDouble != 0 && label.split("&&")[0].equals("geo")){
										Funciones.setCoordinates(radar, latDouble, lonDouble);
									}
								}
								if(dist.get("radares")!=null && !((List<Document>) dist.get("radares")).isEmpty()){
									((List<Document>) dist.get("radares")).add(radar);
								}else{
									dist.append("radares", new ArrayList<Document>(){{add(radar);}});
								}
								distritos.remove(index);
								distritos.add(dist);
							}
						}
					}
					radares.close();
					Funciones.deleteFile(dir.listFiles(fileFilter)[0]);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void generarCatastro(List<Document> distritos) {
		try {
			File dir = new File("./documents/DISTRICT_BARRIO_FORMAT/");
			FileFilter fileFilter = new WildcardFileFilter("*valores-catastrales-barrio.*");
			if(dir.exists() && dir.listFiles(fileFilter).length>0){
				File auxFile = dir.listFiles(fileFilter)[0];
				if(auxFile.exists()){
					CsvReader catastro_barrios = new CsvReader(auxFile.getAbsolutePath(),';');
					catastro_barrios.readHeaders();
					int index = 0;//posicion en la lista del distrito
					int[] dist_barrio_index = null;
					while (catastro_barrios.readRecord()){
						if( (dist_barrio_index = buscarDistritoBarrioInfo(catastro_barrios, 2)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio en el CSV
							index = buscarDistrito_Barrio_Zona(distritos, catastro_barrios.get(dist_barrio_index[0]).trim(), "_id");//obtenemos la posicion en la lista del doc del distrito
							if(index>=0){//LOCALIZAR POR COORDENADAS SI NO TIENE BARRIO O DISTTRITO EN EL CSV!!
								Document dist = distritos.get(index);//cogemos el documento del distrito
								List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
								String aux = catastro_barrios.get(dist_barrio_index[1]).trim();
								int index_b = buscarDistrito_Barrio_Zona(barrios, aux.substring(aux.length()-1), "_id");//posicion en la lista del barrio
								//								System.out.println("INDEX B "+index_b);
								if(index_b>=0){
									Document barrio = barrios.get(index_b);//cogemos el documento del barrio
									List<String> attrZonas = getCampos("catastro", "barrios", null);
									Document catastro = new Document();
									String attr = null;
									for(String label:attrZonas){
										if((attr = buscarValor(catastro_barrios, label.split("&&")[0], label.split("&&")[1]))!=null){
											if(NumberUtils.isNumber(label.split("&&")[1])){
												catastro.append(label.split("&&")[0], Double.parseDouble(attr));
											}else{
												catastro.append(label.split("&&")[0], attr);
											}
										}
									}
									List<Document> auxList = (List<Document>) barrio.get("catastro");
									if(auxList!=null && !auxList.isEmpty()){
										auxList.add(catastro);
										barrio.replace("catastro", auxList);
									}else{
										barrio.append("catastro", new ArrayList<Document>(){{add(catastro);}});
									}
									barrios.remove(index_b);
									barrios.add(barrio);	
									dist.replace("barrios", barrios);	
									distritos.remove(index);//actualizamos
									distritos.add(dist);//añade distrito actualizado	
								}
							}
						}
					}
					catastro_barrios.close();
					Funciones.deleteFile(dir.listFiles(fileFilter)[0]);
					System.out.println("Generados los valores catastrales a nivel de barrio.");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void generarEstaciones(List<Document> distritos) {
		File dir = new File("./documents/ESTACIONES_CALIDAD/");
		try{
			if(dir.exists() && dir.listFiles().length>0){
				formatoFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				addAireAcustica(distritos, dir, new WildcardFileFilter("*calidad-aire.*"), "aire");
				addAireAcustica(distritos, dir, new WildcardFileFilter("*calidad-acustica.*"), "acustico");
				System.out.println("Generadas las estaciones de calidad del aire y ruido junto sus valores actuales.");
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void addAireAcustica(List<Document> distritos, File dir, FileFilter ff, String document) throws IOException, java.text.ParseException{
		if(dir.exists() && dir.listFiles(ff).length>0){
			File auxFile = dir.listFiles(ff)[0];
			if(auxFile.exists()){
				CsvReader estaciones = new CsvReader (auxFile.getAbsolutePath(), ';');
				estaciones.readHeaders();
				while (estaciones.readRecord()){//recorremos el CSV
					String lon = estaciones.get("longitud").replaceAll("\\s+", "");
					String lat = estaciones.get("latitud").replaceAll("\\s+", "");

					double lonDegree = Double.parseDouble(lon.split("[º|°]")[0].replaceAll(",", ".").trim()); 
					double lonMinutes = Double.parseDouble(lon.split("[º|°]")[1].split("'")[0].replaceAll(",", ".").trim());
					double lonSecond = Double.parseDouble(lon.split("[º|°]")[1].split("'")[1].split("[\"|'][O?]")[0].replaceAll(",", ".").replaceAll("'|º|O", "").trim());
					double lonDouble = (-1)*Math.signum(lonDegree) * (Math.abs(lonDegree) + (lonMinutes / 60.0) + (lonSecond / 3600.0));

					double latDegree = Double.parseDouble(lat.split("[º|°]")[0].replaceAll(",", ".").trim()); 
					double latMinutes = Double.parseDouble(lat.split("[º|°]")[1].split("'")[0].replaceAll(",", ".").trim());
					double latSecond = Double.parseDouble(lat.split("[º|°]")[1].split("'")[1].split("[\"|'][N?]")[0].replaceAll(",", ".").replaceAll("'|º|O", "").trim());
					double latDouble = Math.signum(latDegree) * (Math.abs(latDegree) + (latMinutes / 60.0) + (latSecond / 3600.0));

					String CP = geo.getCPbyCoordinates(lonDouble, latDouble);
					if(CP != null && Funciones.checkCP(CP)){
						int index = getDistritoByCP(distritos, CP);//devuelve la posicion que ocupa el distrito con ese CP
						if(index>=0){
							Document dist = distritos.get(index);
							Document estacion = new Document();//documento donde se guardara la info de la estacion
							String attr;
							List<String> attrList = getCampos(document, null, null);
							for(String label:attrList){
								if((attr = buscarValor(estaciones, label.split("&&")[0], label.split("&&")[1]))!=null && !label.split("&&")[0].equals("fecha") && !label.split("&&")[0].equals("geo") && !label.split("&&")[0].equals("valores")){
									if(NumberUtils.isNumber(label.split("&&")[1])){
										estacion.append(label.split("&&")[0], (int)Double.parseDouble(attr));
									}else{
										estacion.append(label.split("&&")[0], attr);
									}
								}else if(label.split("&&")[0].equals("geo")){
									Funciones.setCoordinates(estacion, latDouble, lonDouble);
								}else if(label.split("&&")[0].equals("fecha")){
									estacion.append(label.split("&&")[0], formatoFecha.parse(attr));
								}
							}
							List<Document> valores = new ArrayList<Document>();
							for(String head:estaciones.getHeaders()){//guardamos las medidas tomadas
								if(!head.equals("numero")){
									String value = estaciones.get(head);//cogemos el valor
									if(NumberUtils.isNumber(value.replaceAll(",", "."))){//vemos si es un numero
										valores.add(new Document("id", head).append("valor", Double.parseDouble(value)));
									}
								}
							}
							if(!valores.isEmpty()){
								estacion.append("valores", valores);
							}
							if(dist.get(document)!=null && !((List<Document>) dist.get(document)).isEmpty()){
								((List<Document>) dist.get(document)).add(estacion);
							}else{
								dist.append(document, new ArrayList<Document>(){{add(estacion);}});
							}
							distritos.remove(index);
							distritos.add(dist);
						}
					}
				}
				estaciones.close();
				Funciones.deleteFile(dir.listFiles(ff)[0]);
			}
		}
	}

	private int getDistritoByCP(List<Document> distritos, String CP) {
		for(Document dist:distritos){
			for(Document barrio:(List<Document>)dist.get("barrios")){
				Set<Integer> aux = new HashSet<Integer>((Collection<? extends Integer>) barrio.get("codigo_postal"));
				if(aux.contains(Integer.parseInt(CP))){
					return distritos.indexOf(dist);
				}
			}
		}
		return -1;
	}

	/**
	 * SIMPLIFICAR!! CADA SWITCH EN UNA UNICA FUNCION LO COMUN!! VER NOTEPADD++ CON MAS NOTAS
	 * @param distritos
	 */
	private void generarDistritoFormat(List<Document> distritos) {
		try{
			File folder = new File(".\\documents\\DISTRICT_FORMAT");
			if(folder.exists() && folder.listFiles().length>0){
				List<File> listFiles = new ArrayList<File>();
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
							distritos = addDistritoLoc(distritos, distritos_locs, "censo_animales_domesticos", null);
							listFiles.add(fileEntry);
							break;
						case 1:
							distritos = addDistritoLoc(distritos, distritos_locs, "contenedores", "ropa");
							listFiles.add(fileEntry);
							break;
						case 2:
							distritos = addDistritoLoc(distritos, distritos_locs, "contenedores", "pila");
							listFiles.add(fileEntry);
							break;
						case 3:
							distritos = addDistritoLoc(distritos, distritos_locs, "fuentes_potables", null);
							listFiles.add(fileEntry);
							break;
						case 4:
							distritos = addDistritoLoc(distritos, distritos_locs, "actividades_deportivas", null);
							listFiles.add(fileEntry);
							break;
						default:
							System.out.println("No hay desarrolo para preprocesar "+fileEntry.getName());
						}
						distritos_locs.close();
					}
				}
				for(File fileEntry:listFiles){
					Funciones.deleteFile(fileEntry);
				}
				System.out.println("Generada informacion variada a nivel de distrito.");
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private List<Document> addDistritoLoc(List<Document> distritos, CsvReader distritos_locs, String document, String tipo) throws NumberFormatException, IOException{
		List<String> attrList = getCampos(document, null, null);
		String attr;
		int index = 0;
		int[] dist_index = null;
		if( (dist_index = buscarDistritoBarrioInfo(distritos_locs, 1)) !=null ){//obtenemos la posicion de la cabeceras distrito en el CSV		
			while (distritos_locs.readRecord()){
				Document doc = new Document();//doc a insertar
				if(tipo!=null){//contenedor
					doc.append("tipo", tipo);
				}
				index = buscarDistrito_Barrio_Zona(distritos, distritos_locs.get(dist_index[0]).trim(), "nombre");//obtenemos la posicion en la lista del doc del distrito
				if(index>=0){
					Document dist = distritos.get(index);//cogemos el documento del distrito
					for(String label:attrList){
						if((attr = buscarValor(distritos_locs, label.split("&&")[0], label.split("&&")[1]))!=null && !label.split("&&")[0].equals("geo")){
							if(NumberUtils.isNumber(label.split("&&")[1])){
								doc.append(label.split("&&")[0], Integer.parseInt(attr));
							}else{
								doc.append(label.split("&&")[0], attr);
							}
						}else{
							String lat, lon, east, nort;
							if((lat = buscarValor(distritos_locs, "latitud", "text"))!=null
									&& (lon = buscarValor(distritos_locs, "longitud", "text"))!=null){
								Funciones.setCoordinates(doc, Double.parseDouble(lat.replaceAll(",", "")), Double.parseDouble(lon.replaceAll(",", "")));
							}else if((east = buscarValor(distritos_locs, "coord X", "text"))!=null
									&& (nort = buscarValor(distritos_locs, "coord Y", "text"))!=null){
								LatLon coordinates = UTMCoord.locationFromUTMCoord(30, AVKey.NORTH, Double.parseDouble(east.replaceAll(",", ".")), Double.parseDouble(nort.replaceAll(",", ".")));
								Funciones.setCoordinates(doc, coordinates.getLatitude().getDegrees(), coordinates.getLongitude().getDegrees());
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

	private List<String> getCampos(String partOfJSON, String nivel1, String nivel2){
		List<String> campos = new ArrayList<String>();
		try {
			byte[] encoded = Files.readAllBytes(Paths.get("./extras/JSON_example_TFM.json"));
			Document JSON = Document.parse(new String(encoded, "ISO-8859-1"));
			if(nivel1 != null){//bajamos a subnivel en JSON
				JSON = ((List<Document>) JSON.get(nivel1)).get(0);
				if(nivel2 != null){
					JSON = (Document) JSON.get(nivel2);
				}
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

	private void generarMultas(List<Document> distritos){
		File dir = new File("./documents/UNKNOW_FORMAT/");
		FileFilter fileFilter = new WildcardFileFilter("*multas-circulacion-detalle.*");
		try {
			if(dir.exists() && dir.listFiles(fileFilter).length>0){
				File auxFile = dir.listFiles(fileFilter)[0];
				if(auxFile.exists()){
					String mes = "";
					String anio = "";
					CsvReader multas = new CsvReader (auxFile.getAbsolutePath(), ';');
					multas.readHeaders();
					List<String> attrPadron = getCampos("multas", null, null);
					while (multas.readRecord()){//recorremos el CSV
						String CP = geo.getCPbyStreet(StringUtils.stripAccents(buscarValor(multas, "lugar", "text")));
						if(CP != null && Funciones.checkCP(CP)){
							int index = getDistritoByCP(distritos, CP);//devuelve la posicion que ocupa el distrito con ese CP
							if(index>=0){
								Document dist = distritos.get(index);
								Document doc = new Document();//documenbto a insertar
								for(String label:attrPadron){
									String attr = null;
									switch (label.split("&&")[0]) {
									case "geo":
										String east, nort;
										if((east = buscarValor(multas, "coord X", "text"))!=null
												&& (nort = buscarValor(multas, "coord Y", "text"))!=null){
											LatLon coordinates = UTMCoord.locationFromUTMCoord(30, AVKey.NORTH, Double.parseDouble(east.replaceAll(",", ".")), Double.parseDouble(nort.replaceAll(",", ".")));
											Funciones.setCoordinates(doc, coordinates.getLatitude().getDegrees(), coordinates.getLongitude().getDegrees());
										}
										break;
									case "fecha":
										if((mes = buscarValor(multas, "mes", "text"))!=null
										&& (anio = buscarValor(multas, "anio", "text"))!=null){
											doc.append("fecha",mes+"/"+anio);
										}
										break;
									default:
										if((attr = buscarValor(multas, label.split("&&")[0], label.split("&&")[1]))!=null){
											if(NumberUtils.isNumber(label.split("&&")[1])){
												doc.append(label.split("&&")[0], Integer.parseInt(attr));
											}else{
												doc.append(label.split("&&")[0], attr);
											}
										}
										break;
									}
								}
								List<Document> documents =  (List<Document>) dist.get("multas");
								if(documents!=null){
									documents.add(doc);
									dist.replace("multas", documents);
								}else{
									dist.append("multas", new ArrayList<Document>(){{
										add(doc);}});
								}
								distritos.remove(index);
								distritos.add(dist);
							}
						}
					}
					multas.close();
					Funciones.deleteFile(dir.listFiles(fileFilter)[0]);
					System.out.println("Generadas las multas a nivel de distrtito para fecha: "+mes+"/"+anio+".");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private List<Document> generarDistritosBarrios(){
		//		collection.drop();
		File dir = new File("./documents/DISTRICT_BARRIO_FORMAT/");
		FileFilter fileFilter = new WildcardFileFilter("*padron.*");
		try{
			if(dir.exists() && dir.listFiles(fileFilter).length>0){
				CsvReader distritos_barrios = new CsvReader (dir.listFiles(fileFilter)[0].getAbsolutePath(), ';');
				distritos_barrios.readHeaders();
				List<Document> distritos = new ArrayList<>();
				List<String> attrPadron = getCampos("padron", "barrios", null);
				int[] dist_barrio_index = null;
				while (distritos_barrios.readRecord()){//recorremos el CSV
					int index = 0;//posicion en la lista del distrito
					if( (dist_barrio_index = buscarDistritoBarrioInfo(distritos_barrios, 2)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio
						if(distritos.isEmpty() || (index = buscarDistrito_Barrio_Zona(distritos, distritos_barrios.get(dist_barrio_index[0]).trim(), "nombre"))<0){//distrito nuevo
							Document dist = new Document("_id", distritos_barrios.get("COD_DISTRITO")).append("nombre", distritos_barrios.get(dist_barrio_index[0]).trim());//cogemos el documento del distrito
							List<Document> barrios = new ArrayList<Document>();//lista de barrios del sitrito
							Document bar = completarBarrio(distritos_barrios, dist_barrio_index);
							bar = completarBarrio(distritos_barrios, dist_barrio_index);
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
							if(!barrios.isEmpty() && (index_b = buscarDistrito_Barrio_Zona(barrios, distritos_barrios.get(dist_barrio_index[1]).trim(), "nombre"))>=0){//ya contiene ese barrio
								Document bar = barrios.get(index_b);
								List<Document> padron = (List<Document>) bar.get("padron");
								if(padron!=null){
									boolean cambiado = false;
									for(Document pad:padron){
										if(pad.getInteger("cod_edad")==Integer.parseInt((buscarValor(distritos_barrios, "cod_edad", "0")))){
											//actualizar padron
											for(String label:attrPadron){
												if(!label.split("&&")[0].equals("cod_edad")){
													String valor;
													if((valor = buscarValor(distritos_barrios, label.split("&&")[0], label.split("&&")[1]))!=null){
														if(pad.get(label.split("&&")[0])!=null){
															pad.replace(label.split("&&")[0], pad.getInteger(label.split("&&")[0])+Integer.valueOf(valor));
														}else{//todavia no se tomo valores para ese tipo
															pad.append(label.split("&&")[0], Integer.valueOf(valor));
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
								Document bar = completarBarrio(distritos_barrios, dist_barrio_index);
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
				distritos_barrios.close();
				Funciones.deleteFile(dir.listFiles(fileFilter)[0]);
				File dirAux = new File("./documents/DISTRICT_BARRIO_FORMAT/");
				FileFilter fileFilterAux = new WildcardFileFilter("*distritos-barrios.*");
				Funciones.deleteFile(dirAux.listFiles(fileFilterAux)[0]);
				return distritos;
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private Document completarBarrio(CsvReader distritos_barrios, int[] dist_barrio_index) throws IOException {
		String nombre =  distritos_barrios.get(dist_barrio_index[1]).trim();
		Document barrio = new Document("_id", distritos_barrios.get("COD_BARRIO")).append("nombre", nombre);
		File dir = new File("./documents/DISTRICT_BARRIO_FORMAT/");
		FileFilter fileFilter = new WildcardFileFilter("*distritos-barrios.*");
		CsvReader info_barrio = new CsvReader (dir.listFiles(fileFilter)[0].getAbsolutePath(), ';');
		info_barrio.readHeaders();
		while (info_barrio.readRecord()){//recorremos el CSV
			if(buscarValor(info_barrio, "nombre barrio", "text").equals(nombre)){
				barrio.append("superfice", Double.parseDouble(buscarValor(info_barrio, "superfice", "1")));
				barrio.append("perimetro", Double.parseDouble(buscarValor(info_barrio, "perimetro", "1")));
				break;
			}
		}
		info_barrio.close();
		return barrio;
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
				if(NumberUtils.isNumber(label.split("&&")[1])){
					pad.append(label.split("&&")[0], Integer.parseInt(attr));
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
	private void generarZonas(List<Document> distritos){
		try{
			File folder = new File(".\\documents\\PK_FORMAT");
			if(folder.exists() && folder.listFiles().length>0){//hay contenido en la carpeta
				List<File> listFiles = new ArrayList<File>();
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
								if(index>=0){//LOCALIZAR POR COORDENADAS O CP SI NO TIENE BARRIO O DISTTRITO EN EL CSV!!
									Document dist = distritos.get(index);//cogemos el documento del distrito
									List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
									int index_b = buscarDistrito_Barrio_Zona(barrios, distritos_zonas.get(dist_barrio_index[1]).trim(), "nombre");//posicion en la lista del barrio
									if(index_b>=0){
										Document barrio = barrios.get(index_b);//cogemos el documento del barrio
										String cod_postal = buscarValor(distritos_zonas, "codigo_postal", "1");
										if(cod_postal!=null && !cod_postal.equals("") && Funciones.checkCP(cod_postal)){//codigo postal no valido o nulo
											if(barrio.get("codigo_postal")!=null&&!barrio.get("codigo_postal").equals("")){
												Set<Integer> aux = new HashSet<Integer>((Collection<? extends Integer>) barrio.get("codigo_postal"));
												aux.add(Integer.parseInt(cod_postal));
												barrio.replace("codigo_postal", aux);
												//												((Set<Integer>)((Collection<? extends Integer>)barrio.get("codigo_postal"))).add();
											}else{
												barrio.append("codigo_postal",  new HashSet<Integer>(){{
													add(Integer.parseInt(cod_postal));}});
											}
											if(barrio.get("zonas")!=null&&!barrio.get("zonas").equals("")){//ya hay zonas guardadas para ese barrio
												int index_z;
												if((index_z = buscarDistrito_Barrio_Zona((List<Document>) barrio.get("zonas"), distritos_zonas.get("PK"), "PK")) < 0){
													((List<Document>) barrio.get("zonas")).add(addZona(distritos_zonas, topics));
												}else{
													if(topics!=null && !topics.isEmpty()){
														if(((Document)((List<Document>) barrio.get("zonas")).get(index_z)).get("rol")!=null){
															for(String top:topics){
																((Set<String>)((Document)((List<Document>) barrio.get("zonas")).get(index_z)).get("rol")).add(top);
															}
														}else{//no tiene rol la zona
															((Document)((List<Document>) barrio.get("zonas")).get(index_z)).append("rol", topics);
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
											distritos.add(dist);//añade distrito actualizado	
										}
									}	
								}
							}
						}
						distritos_zonas.close();
						listFiles.add(fileEntry);
					}
				}
				for(File fileEntry:listFiles){
					Funciones.deleteFile(fileEntry);
				}
				System.out.println("Generadas las zonas con formato PK a nivel de barrio.");
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
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
		List<String> attrZonas = getCampos("zonas", "barrios", null);
		Document zona = new Document();
		String attr = null;
		for(String label:attrZonas){
			attr = buscarValor(distritos_zonas, label.split("&&")[0], label.split("&&")[1]);
			if(attr!=null&&!label.split("&&")[0].equals("geo")&&!label.split("&&")[0].equals("rol")){
				if(NumberUtils.isNumber(label.split("&&")[1])){
					zona.append(label.split("&&")[0], Integer.parseInt(attr));
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
						Funciones.setCoordinates(zona, Double.parseDouble(lat.replaceAll(",", "")), Double.parseDouble(lon.replaceAll(",", "")));
					}
				}
			}
		}
		return zona;
	}

	private String buscarValor(CsvReader csvDoc, String aprox, String tipo) {
		try{
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
		}catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Dado un document CSV devuelve la posicion de las cabeceras distrito y/o barrio
	 * @param distritos_barrios - CSV a leer
	 * @param cnt_main - Indica si se quiere las dos posiciones o solo 1 de las dos.
	 * @return Array con posiciones que ocupan. 0- Distrito y 1- Barrio.
	 */
	private int[] buscarDistritoBarrioInfo(CsvReader distritos_barrios, int cnt_main) {
		int[] dist_barrio_Index = new int[2];
		int cnt = 0;
		try{
			String[] headers = distritos_barrios.getHeaders();
			for(int i = 0; (i<headers.length)&&(cnt<2); i++){
				if(headers[i].toLowerCase().equals("distrito")||headers[i].toLowerCase().equals("desc_distrito")||headers[i].toLowerCase().equals("distrito_cod")){
					dist_barrio_Index[0] = i;
					cnt++;
				}else if(headers[i].toLowerCase().equals("barrio")||headers[i].toLowerCase().equals("desc_barrio")||headers[i].toLowerCase().equals("barrio_cod")){
					dist_barrio_Index[1] = i;
					cnt++;
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		if(cnt>=cnt_main){
			return dist_barrio_Index;
		}
		return null;
	}

	/**
	 * Dada una lista de documentos de distritos/barrios/zonas devuelve la posicion que ocupa en ella
	 * @param distritos_barrios: lista de distritos, barrios o zonas
	 * @param code: valor del campo a buscar
	 * @param id: nombre del campo en el documento
	 * @return: Posicion en la lista del documento buscado
	 */
	private int buscarDistrito_Barrio_Zona(List<Document> distritos_barrios_zonas, String code, String id){
		for(Document dist_bar_zon:distritos_barrios_zonas){
			if(dist_bar_zon.get(id).equals(code) || dist_bar_zon.get(id).toString().split("-")[0].equals(code)
					|| (!id.equals("PK") && Funciones.similarity(dist_bar_zon.get(id).toString(), code) > 0.8)){
				return distritos_barrios_zonas.indexOf(dist_bar_zon);
			}
		}
		return -1;
	}


	private void guardarElecciones(String document, List<Document> distritos) {
		try {
			File dir = new File("./documents/DISTRICT_BARRIO_FORMAT/");
			FileFilter fileFilter = new WildcardFileFilter(document);
			if(dir.exists() && dir.listFiles(fileFilter).length>0){
				File auxFile = dir.listFiles(fileFilter)[0];
				if(auxFile.exists()){
					CsvReader elecciones = new CsvReader(auxFile.getAbsolutePath(),';');
					elecciones.readHeaders();
					int index = 0;//posicion en la lista del distrito
					int[] dist_barrio_index = null;
					while (elecciones.readRecord()){
						if( (dist_barrio_index = buscarDistritoBarrioInfo(elecciones, 2)) !=null ){//obtenemos la posicion de las cabeceras nombre distrito y barrio en el CSV
							index = buscarDistrito_Barrio_Zona(distritos, elecciones.get(dist_barrio_index[0]).trim(), "_id");//obtenemos la posicion en la lista del doc del distrito
							//							System.out.println("INDEX D "+index);
							if(index>=0){//LOCALIZAR POR COORDENADAS SI NO TIENE BARRIO O DISTTRITO EN EL CSV!!
								Document dist = distritos.get(index);//cogemos el documento del distrito
								List<Document> barrios = (List<Document>) dist.get("barrios");//cogemos su lista de barrios asociada al distrito
								int index_b = buscarDistrito_Barrio_Zona(barrios, elecciones.get(dist_barrio_index[1]).trim(), "_id");//posicion en la lista del barrio
								//								System.out.println("INDEX B "+index_b);
								if(index_b>=0){
									Document barrio = barrios.get(index_b);//cogemos el documento del barrio
									List<String> attrZonas = getCampos("elecciones", "barrios", null);
									Document votos = new Document();
									String attr = null;
									for(String label:attrZonas){
										attr = buscarValor(elecciones, label.split("&&")[0], label.split("&&")[1]);
										if(attr!=null&&!label.split("&&")[0].equals("votos")){
											votos.append(label.split("&&")[0], Integer.parseInt(attr));
										}else if(label.split("&&")[0].equals("votos")){
											List<Document> votos_partidos = new ArrayList<Document>();
											for(int i = 7; i<elecciones.getHeaderCount(); i++){
												votos_partidos.add(new Document()
														.append("partido", elecciones.getHeaders()[i])
														.append("total", elecciones.get(i)) );
											}
											votos.append(label.split("&&")[0], votos_partidos);
										}
									}
									barrio.append("elecciones", votos);
									barrios.remove(index_b);
									barrios.add(barrio);	
									dist.replace("barrios", barrios);	
									distritos.remove(index);//actualizamos
									distritos.add(dist);//añade distrito actualizado	
								}
							}
						}
					}
					elecciones.close();
					Funciones.deleteFile(dir.listFiles(fileFilter)[0]);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		//		System.out.println(NumberUtils.isNumber("222"));
		//				Almacenar alm = new Almacenar(null);
		//		alm.guardarElecciones("elecciones-ayuntamiento-madrid", null);

		//				Almacenar alm = new Almacenar(null);
		//				List<String> al = alm.getCampos("valores", "aire");
		//				for(String a:al){
		//					System.out.println(a);
		//				}
		//	alm.generarZonas(null);
		//		alm.generarDistritosBarrios();
		//		String a = "11,42\"O";
		//		String a = "11,42''O";
		//		String a = "11,42'ODSfvsvdf";
		//		String a = "25Oº";
		//		System.out.println(a.split("[º|°]")[0].replaceAll(",", ".").trim());
		//		System.out.println(a.split("[\"|'][O?]")[0].replaceAll("',", ".").replaceAll("'|º|O", "").trim());

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
//														String a = "Nº Plazas por color";
//														String b = "n_plazas_color";
//														System.out.println(Funciones.similarity(a, b));
		// omp parallel for schedule(dynamic)
		//        for (int i = 2; i < 20; i += 3) {
		//            System.out.println("  @" + i);
		//        }
	}
}
