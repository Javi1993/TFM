package preprocesamiento;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.csvreader.CsvReader;

public class Limpieza {

	public void separacionCarpetas(String path){
		try {
			File folder = new File(path);
			for (File fileEntry : folder.listFiles()) {
				if (!fileEntry.isDirectory()) {
					File src = new File(folder+File.separator+fileEntry.getName());
					CsvReader dataset = new CsvReader(folder+File.separator+fileEntry.getName(), ';');
					dataset.readHeaders();
					if(dataset.getHeaders()[0].equals("PK")){//archivos formato PK
						dataset.close();	
						File dest = new File(folder+File.separator+"PK_FORMAT"+File.separator+fileEntry.getName());
						FileUtils.copyFile(src, dest);
						FileUtils.forceDelete(src);
					}else{
						boolean barrio = false;
						boolean distrito = false;
						for(int i = 0; i<dataset.getHeaders().length; i++){
							if(!barrio||!distrito){
								if(dataset.getHeaders()[i].toLowerCase().contains("barrio")){
									barrio = true;
								}
								if(dataset.getHeaders()[i].toLowerCase().contains("distrito")){
									distrito = true;
								}
							}else{
								break;
							}
						}
						dataset.close();
						File dest = null;
						if(!barrio&&distrito){//archivos con solo distrito
							dest = new File(folder+File.separator+"DISTRICT_FORMAT"+File.separator+fileEntry.getName());
						}else if(barrio&&distrito){
							dest = new File(folder+File.separator+"DISTRICT_BARRIO_FORMAT"+File.separator+fileEntry.getName());
						}else if(barrio&&!distrito){	
							dest = new File(folder+File.separator+"BARRIO_FORMAT"+File.separator+fileEntry.getName());
						}else{
							if(fileEntry.getName().contains("calidad-aire")||fileEntry.getName().contains("calidad-acustica")){
								dest = new File(folder+File.separator+"ESTACIONES_CALIDAD"+File.separator+fileEntry.getName());
							}else{
								dest = new File(folder+File.separator+"UNKNOW_FORMAT"+File.separator+fileEntry.getName());
							}
						}
						FileUtils.copyFile(src, dest);//copiamos archivo
						FileUtils.forceDelete(src);//borramos origen
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
