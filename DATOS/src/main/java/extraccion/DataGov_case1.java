package extraccion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;

import funciones.Funciones;

public class DataGov_case1 {

	public DataGov_case1(){
		String  thisLine = null;
		String path = System.getProperty("extras")+"url_data_gov_case1.txt";//documento con las URLs de los datos
		int i = 0;
		try{
			String[] urls = new String[Funciones.getLineNumber(path)];
			BufferedReader br = new BufferedReader(new FileReader(path));
			while ((thisLine = br.readLine()) != null) {//se insertan las url que contiene
				urls[i] = thisLine;
				i++;
			}
			br.close();
			for(int j = 0; j<urls.length; j++){//se recorre el array 
				File fileDest = new File(System.getProperty("documents")+"Crimes_2001.csv");
				FileUtils.copyURLToFile(new URL(urls[j]), fileDest);//se descarga el contenido y se guarda en el fichero
				System.out.println("Se ha descargado '"+fileDest.getName()+"'.");
			}
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
}
