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
					CsvReader products = new CsvReader(folder+File.separator+fileEntry.getName(), ';');
					products.readHeaders();
					if(products.getHeaders()[0].equals("PK")){//archivos formato PK
						products.close();	
						File dest = new File(folder+File.separator+"PK_FORMAT"+File.separator+fileEntry.getName());
						FileUtils.copyFile(src, dest);
						FileUtils.forceDelete(src);
					}else{
						boolean barrio = false;
						boolean distrito = false;
						for(int i = 0; i<products.getHeaders().length; i++){
							if(!barrio||!distrito){
								if(products.getHeaders()[i].toLowerCase().contains("barrio")){
									barrio = true;
								}
								if(products.getHeaders()[i].toLowerCase().contains("distrito")){
									distrito = true;
								}
							}else{
								break;
							}
						}
						if(!barrio&&distrito){//archivos con solo distrito
							products.close();	
							File dest = new File(folder+File.separator+"DISTRICT_FORMAT"+File.separator+fileEntry.getName());
							FileUtils.copyFile(src, dest);
							FileUtils.forceDelete(src);
						}else if(barrio&&distrito){
							products.close();	
							File dest = new File(folder+File.separator+"DISTRICT_BARRIO_FORMAT"+File.separator+fileEntry.getName());
							FileUtils.copyFile(src, dest);
							FileUtils.forceDelete(src);
						}else if(barrio&&!distrito){
							products.close();	
							File dest = new File(folder+File.separator+"BARRIO_FORMAT"+File.separator+fileEntry.getName());
							FileUtils.copyFile(src, dest);
							FileUtils.forceDelete(src);
						}else{
							products.close();	
							File dest = new File(folder+File.separator+"UNKNOW_FORMAT"+File.separator+fileEntry.getName());
							FileUtils.copyFile(src, dest);
							FileUtils.forceDelete(src);
						}
					}
				}
			}
			//			CsvReader products = new CsvReader(".\\documents\\200186-0-polideportivos.csv", ';');
			//			products.readHeaders();
			//			System.out.println("----------------------------INI HEADERD");
			//			System.out.println(products.getHeaders()[0]);
			//			System.out.println("----------------------------FIN HEADERD");
			//			while (products.readRecord()){
			//				System.out.println("++++++++++++++++++++++++");
			//				System.out.println(products.getRawRecord());
			//			}
			//			products.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args)  {
		Limpieza pre = new Limpieza();
		pre.separacionCarpetas(".\\documents");
	}
}
