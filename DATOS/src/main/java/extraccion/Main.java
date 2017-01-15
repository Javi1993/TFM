package extraccion;

import funciones.Funciones;
import preprocesamiento.Almacenar;
import preprocesamiento.Limpieza;

public class Main {
	
	public static void main(String[] args) {
		Funciones.loadPropierties();
		DatosGobES dgES = new DatosGobES();
		new Mambiente();
		new Limpieza().separacionCarpetas();
		new Almacenar(dgES.getDataset_ID());
		Funciones.vaciarDocuments();
	}
}

