package funciones;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Funciones {

	/**
	 * Devuelve el numero de lineas del fichero pasado
	 * @param path - localizacion del fichero
	 * @return Numero de lineas
	 * @throws IOException
	 */
	public static int getLineNumber(String path) {
		int nlines = 0;
		try{
			LineNumberReader lnr = new LineNumberReader(new FileReader(new File(path)));	
			lnr.skip(Long.MAX_VALUE);
			nlines = lnr.getLineNumber() + 1; //Add 1 because line index starts at 0
			lnr.close();
		}catch (IOException e) {
			System.err.println("No se encuentra el archivo '"+path+"'.");
		}
		return nlines;
	}

	/**
	 * Devuelve la similitud entre dos String en base a la distancia Levenshtein
	 * @param s1 - string 1
	 * @param s2 - string 2
	 * @return Similitud entre 0-1
	 */
	public static double similarity(String s1, String s2) {
		String longer = s1.toLowerCase(), shorter = s2.toLowerCase();
		if (s1.length() < s2.length()) { // longer should always have greater length
			longer = s2.toLowerCase(); shorter = s1.toLowerCase();
		}
		int longerLength = longer.length();
		if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
		return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) / (double) longerLength;
	}

	/**
	 * Comprueba si el codigo postal pasado es de la ciudad de Madrid
	 * @param CP - Codigo postal
	 * @return - booleano con respuesta
	 * @throws IOException
	 */
	public static boolean checkCP(String CP) throws IOException{
		UrlValidator defaultValidator = new UrlValidator(); 
		Document doc = null;
		String url = "http://www.codigospostales.com/mapa.cgi?codigo="+CP;
		try{
			if (defaultValidator.isValid(url)) {
				doc = Jsoup.connect(url).timeout(30000).get();
				Element content = doc.select("h1").first();
				String[] texto = content.text().split(",");
				String city = texto[texto.length-1].trim();
				if(city.toLowerCase().equals("madrid")){
					return true;
				}
			}
		}catch (SocketTimeoutException e) {
			System.err.println("Se ha excedido el tiempo para validar el codigo postal '"+CP+"' en la URL '"+url+"'.");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Inserta la geolocalizacion en un documento JSON
	 * @param doc - documento JSON
	 * @param lat - latitud
	 * @param lon - longitud
	 */
	@SuppressWarnings("serial")
	public static void setCoordinates(org.bson.Document doc, double lat, double lon){
		if(lat>40 && lat<41 && lon<-3 && lon>-4){//comprobamos que no hay coordenadas erroneas
			doc.append("geo", new org.bson.Document("type","Point")
					.append("coordinates", new ArrayList<Double>(){{
						add(lon);
						add(lat);
					}}));
			doc.append("latitud", lat).append("longitud", lon);
		}
	}
	
	@SuppressWarnings("serial")
	public static void setCoordinatesAux(org.bson.Document doc, double lat, double lon){
			doc.append("geo", new org.bson.Document("type","Point")
					.append("coordinates", new ArrayList<Double>(){{
						add(lon);
						add(lat);
					}}));
			doc.append("latitud", lat).append("longitud", lon);
	}

	/**
	 * Comprueba si el nombre del fichero pasado como parametro ya se ha descargado con anterioridad
	 * @param fileName - Nombre del fichero a descargar
	 * @return - Booleano indicando respuesta
	 */
	public static boolean checkNew(String fileName) {
		File dir = new File("."+File.separator+"documents"+File.separator+"HISTORICO"+File.separator);//directorio con el historico de ficheros descargados
		if(dir.exists()){
			FileFilter fileFilter = new WildcardFileFilter("*"+fileName.substring(0,fileName.lastIndexOf('.'))+".zip");//patron de busqueda
			if(dir.listFiles(fileFilter).length>0){
				return true;//existe un fichero con ese nombre
			}
		}
		return false;//no existe
	}

	/**
	 * Borra los directorios y ficheros almacenados en la carpeta documents
	 */
	public static void vaciarDocuments() {
		File folder = new File("."+File.separator+"documents"+File.separator);
		for (File fileEntry : folder.listFiles()) {
			if (!fileEntry.getName().equals("HISTORICO")){//si es diferente de la carpeta hitorico eliminamos
				try {
					FileUtils.deleteDirectory(fileEntry);
				} catch (IOException e){
					if (fileEntry.isDirectory()){
						System.err.println("No se puede borrar el directorio '"+fileEntry.getName()+".\n"+e.getMessage());
					}else{
						System.err.println("No se puede borrar el fichero '"+fileEntry.getName()+".\n"+e.getMessage());
					}
				}
			}
		}
	}

	/**
	 * Añade al document los atributos comunes que se han extraido del dataset
	 * @param doc - documento JSON
	 * @param attr - atributo a añadir
	 * @param label - campo
	 */
	@SuppressWarnings("deprecation")
	public static void setComunAttr(org.bson.Document doc, String attr, String label) {
		if(NumberUtils.isNumber(label.split("&&")[1])){
			doc.append(label.split("&&")[0], Double.parseDouble(attr));
		}else{
			doc.append(label.split("&&")[0], attr);
		}
	}

	/**
	 * Borra el fichero recibido
	 * @param fileEntry - fichero origen
	 * @throws IOException
	 */
	public static void deleteFile(File fileEntry) throws IOException{
		File dest = new File("./documents/HISTORICO/"+fileEntry.getName().substring(0,fileEntry.getName().lastIndexOf('.'))+".zip");
		File src = new File(fileEntry.getPath());
		compressFile(dest, src);
		FileUtils.forceDelete(src);//borramos origen
	}

	/**
	 * Comprime un fichero en formato ZIP
	 * @param dest - fichero destino
	 * @param src - fichero origen
	 * @throws IOException
	 */
	private static void compressFile(File dest, File src) throws IOException{
		FileOutputStream fos = new FileOutputStream(dest);
		ZipOutputStream out = new ZipOutputStream(fos);
		ZipEntry e = new ZipEntry(src.getName());
		out.putNextEntry(e);
		FileInputStream fis =  new FileInputStream(src);
		byte[] data =  IOUtils.toByteArray(fis);
		out.write(data, 0, data.length);
		out.closeEntry();
		out.close();
		fos.close();
		fis.close();
	}

	/**
	 * Asigna como propiedades de la aplicacion variables como rutas, usuarios, db, etc. Para de esta manera tener un unico origen
	 */
	public static void loadPropierties() {
		System.setProperty("server", "localhost");
		System.setProperty("db", "tfm");
		System.setProperty("colection", "distritos");
		System.setProperty("extras", "."+File.separator+"extras"+File.separator);
		System.setProperty("documents", "."+File.separator+"documents"+File.separator);
		System.setProperty("pk", System.getProperty("documents")+"PK_FORMAT"+File.separator);
		System.setProperty("district_barrio_format", System.getProperty("documents")+"DISTRICT_BARRIO_FORMAT"+File.separator);
		System.setProperty("barrio_format", System.getProperty("documents")+"BARRIO_FORMAT"+File.separator);
		System.setProperty("district_format", System.getProperty("documents")+"DISTRICT_FORMAT"+File.separator);
		System.setProperty("estaciones_calidad", System.getProperty("documents")+"ESTACIONES_CALIDAD"+File.separator);
		System.setProperty("unknow_format", System.getProperty("documents")+"UNKNOW_FORMAT"+File.separator);
	}
}
