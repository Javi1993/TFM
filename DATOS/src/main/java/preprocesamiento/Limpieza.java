package preprocesamiento;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

@SuppressWarnings("deprecation")
public class Limpieza {

	public void separacionCarpetas(String path){
		try {
			File folder = new File(path);
			transformarExcelToCSV(folder);//transformamos los Excels a formato CSV
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

	private void transformarExcelToCSV(File folder) {
		for (File fileEntry : folder.listFiles()) {
			try {
				if (!fileEntry.isDirectory() && FilenameUtils.getExtension(fileEntry.getAbsolutePath()).equals("xls")) {
					//De momento solo elecciones
					if(fileEntry.getName().contains("elecciones-ayuntamiento-madrid")){
						leerExcelElecciones(fileEntry);
					}
				}
			} catch (IOException e) {
				System.err.println("Error al leer '"+fileEntry.getName()+"'.");
				e.printStackTrace();
			}
		}

	}

	private void leerExcelElecciones(File fileEntry) throws IOException {
		String[] headers = new String[]{"distrito","barrio","censo (1)","abstención","total","nulos","blanco","PP","PSOE","Ahora Madrid","Ciudadanos","AES","PH","IUCM-LV","UPyD","ULEG","P-LIB","LV-GV","LCN","PCAS-TC-PACTO","MJS","SAIn","PACMA","PCPE","VOX","POSI","EB","FE DE LAS JONS","CILUS"};
		POIFSFileSystem fs = new POIFSFileSystem(fileEntry);
		HSSFWorkbook wb = new HSSFWorkbook(fs);
		HSSFSheet sheet = wb.getSheetAt(0);
		HSSFRow row;
		HSSFCell cell;
		boolean contenidoBueno = false;
		short longitud = -1;
		int[] columnIndexHeaders = new int[headers.length];
		int rowIndexHeaders = 0;
		int indexHeader = 0;
		int rows; // No of rows
		rows = sheet.getPhysicalNumberOfRows();
		int cols = 0; // No of columns
		int tmp = 0;

		// This trick ensures that we get the data properly even if it doesn't start from first few rows
		for(int i = 0; i < 10 || i < rows; i++) {
			row = sheet.getRow(i);
			if(row != null) {
				tmp = sheet.getRow(i).getPhysicalNumberOfCells();
				if(tmp > cols) cols = tmp;
			}
		}
		List<List<String>> mesas = new ArrayList<List<String>>();
		for(int r = 0; r < rows; r++) {
			List<String> mesa = new ArrayList<String>();
			row = sheet.getRow(r);
			if(row != null && (longitud == -1|| row.getLastCellNum()==longitud)) {
				for(int c = 0; c < cols; c++) {
					cell = row.getCell((short)c);
					if(cell != null && (cell.toString().toLowerCase().equals("distrito") || contenidoBueno)) {
						//							System.out.println(cell.toString().toLowerCase().trim());
						if((cell.toString().toLowerCase().equals("distrito") || cell.toString().toLowerCase().equals("nº")) && !contenidoBueno){
							longitud = row.getLastCellNum();
							contenidoBueno = true;
							columnIndexHeaders[0] = cell.getColumnIndex();
							rowIndexHeaders = cell.getRowIndex()+6;
						}else if((indexHeader = isHeaderChoosen(headers, cell.toString().toLowerCase().trim())) > 0){
							if(columnIndexHeaders[indexHeader] == 0){
								columnIndexHeaders[indexHeader] = cell.getColumnIndex();
							}
						}else if(cell.getRowIndex()>rowIndexHeaders && columnIndexHeaders[columnIndexHeaders.length-1]!=0){//entramos en contenido
							int j = 0;
							if((j =isCellChoosen(columnIndexHeaders, cell.getColumnIndex()))>=0){
								if(j <= 1){
									String cellAux = cell.getStringCellValue();
									if(NumberUtils.isNumber(cellAux)){
										mesa.add(cell.getStringCellValue());
									}else{//fin de recuento votos
										break;
									}
								}else{
									mesa.add(String.valueOf(cell.getNumericCellValue()));
								}
							}
						}	
					}
				}			
			}
			mesas.add(mesa);
		}
		wb.close();
		fs.close();
		FileUtils.forceDelete(fileEntry);
		volcarCSV(fusionarFilas(mesas), headers, fileEntry.getName().substring(0,fileEntry.getName().lastIndexOf(".")));
	}

	private List<List<String>> fusionarFilas(List<List<String>> mesas) {
		String barrio = "";
		String distrito = "";
		int size = 0;
		List<List<String>> mesasAux = new ArrayList<List<String>>();
		for(List<String> mesa:mesas){//fusionamos los resultados de las mesas para dejar un registro por barrio
			if(mesa!=null && !mesa.isEmpty()){
				if(mesa.get(1).equals(barrio) && mesa.get(0).equals(distrito)){
					List<String> valorAux = mesasAux.get(size-1);
					for(int i = 2; i<mesa.size(); i++){
						double suma = (int)Double.parseDouble(valorAux.get(i))+(int)Double.parseDouble(mesa.get(i));
						valorAux.remove(i);
						valorAux.add(i, String.valueOf(suma));
					}
					mesasAux.remove(size-1);
					mesasAux.add(size-1, valorAux);
				}else{
					List<String> valorAux = new ArrayList<String>();
					barrio = mesa.get(1);
					distrito = mesa.get(0);
					for(String valores:mesa){
						valorAux.add(valores);
					}
					mesasAux.add(valorAux);
					size = mesasAux.size();
				}
			}
		}
		mesas.clear();
		return mesasAux;
	}

	private void volcarCSV(List<List<String>> list, String[] headers, String name) {
		String outputFile = "./documents/"+name+".csv";
		try {
			if(list != null && !list.isEmpty()){
				CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, false), ';');
				for (String head: headers){
					csvOutput.write(head.replaceAll("[(][\\d][)]$", "").trim().toLowerCase());
				}
				csvOutput.endRecord();
				for(List<String> mesa:list){
					for (String valor:mesa){
						if(mesa.indexOf(valor)==1){
							csvOutput.write(valor.substring(valor.length()-1));
						}else{
							csvOutput.write(String.valueOf((int)Double.parseDouble(valor)));
						}		
					}
					csvOutput.endRecord();
				} 
				csvOutput.close();
				list.clear();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private int isCellChoosen(int[] headerIndex, int columnIndex){
		for(int i = 0; i<headerIndex.length; i++){
			if(columnIndex == headerIndex[i]){
				return i;
			}
		}
		return -1;
	}

	private int isHeaderChoosen(String[] headers, String cell){
		int i = 0;
		for(i = 0; i<headers.length; i++){
			if(cell.equals(headers[i].toLowerCase())){
				return i;
			}
		}
		return 0;
	}
}
