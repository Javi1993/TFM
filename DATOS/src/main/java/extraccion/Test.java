package extraccion;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Test {
	
	public static void main (String[] args) throws Exception{
		testGet();
	}
	
	public static void testGet() throws Exception{
	      StringBuilder result = new StringBuilder();
	      URL url = new URL("http://datos.gob.es/apidata/catalog/distribution/dataset/l01280796-padron-municipal-historico?_sort=-title&_pageSize=10&_page=0");
	      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	      conn.setRequestMethod("GET");
	      conn.setRequestProperty("Content-Type", 
	    	        "application/json");
	      BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	      String line;
	      while ((line = rd.readLine()) != null) {
	         result.append(line);
	      }
	      rd.close();
	      
	      JSONObject jsonObj = new JSONObject(result.toString());
	      JSONArray urls = (JSONArray) ((JSONObject)jsonObj.get("result")).get("items");
	      //org.jsoup.nodes.Document doc = Jsoup.connect("http://jsoup.org").get();
	      //Document doc = new Document();
	      //System.out.println(urls.length());
//	      System.out.println(urls.get(0).toString());
	      Document doc = Jsoup.connect(urls.get(0).toString()).get();
	      Element table = doc.select("table").first();
	      Element td = table.select("td").first();
	      Element a = td.select("a").first();
//	      System.out.println(a.attr("href"));
	      String link = a.attr("href");
	      
	      File csv = new File("D:\\t710908\\Downloads\\padron.csv");
	      FileUtils.copyURLToFile(new URL(link), csv);
	}
}