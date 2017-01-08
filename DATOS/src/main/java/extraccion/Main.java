package extraccion;

import java.io.File;
import funciones.Funciones;
import preprocesamiento.Almacenar;
import preprocesamiento.Limpieza;

public class Main {
	
	static private String path = "."+File.separator+"documents";//raiz de los datasets descargados
	
	public static void main(String[] args) {
		Funciones.loadPropierties();
		DatosGobES dgES = new DatosGobES();
		new Mambiente();
		new Limpieza().separacionCarpetas(path);
		new Almacenar(dgES.getDataset_ID());
		Funciones.vaciarDocuments();
	}
}

