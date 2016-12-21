package preprocesamiento.meaningcloud;

/**
 * Text Classification 1.1 starting client for Java.
 *
 * In order to run this example, the license key must be included in the key variable.
 * If you don't know your key, check your personal area at MeaningCloud (https://www.meaningcloud.com/developer/account/subscription)
 *
 * Once you have the key, edit the parameters and call "javac *.java; java ClassClient"
 *
 * You can find more information at http://www.meaningcloud.com/developer/text-classification/doc/1.1
 *
 * @author     MeaningCloud
 * @contact    http://www.meaningcloud.com 
 * @copyright  Copyright (c) 2015, MeaningCloud LLC All rights reserved.
 */
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import java.io.ByteArrayInputStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.*;


/**
 * This class implements a starting client for Text Classification  
 */
public class ClassClient {

	public List<String> tematicaDataset(String ID){
		// We define the variables needed to call the API
		String api = "http://api.meaningcloud.com/class-1.1";
		String key = "67d2d31e37c2ba1d032188b1233f19bf";
		String txt = ID;
		//		String model = "IPTC_es";  // IPTC_es/IPTC_en/IPTC_fr/IPTC_it/IPTC_ca/EUROVOC_es_ca/BusinessRep_es/BusinessRepShort_es
		//USAR  IPTC_es y coger por relevancia!
		//SINO DEVUELVE NADA COGER OTRA QUE SI E IGUAL, X RELEVANCIA
		List<String> topicsFinal = new ArrayList<String>() ;
		try{

			Post post = new Post (api);
			post.addParameter("key", key);
			post.addParameter("txt", txt);
			post.addParameter("of", "json");
			//String response = post.getResponse();

			
			String[] tiposModel = new String[]{"IPTC_es","SocialMedia_es","EUROVOC_es_ca"};//checkear 2º y 3º
			for(int i = 0; i<tiposModel.length||topicsFinal.size()<2; i++){
				List<String> topicsAux = busquedaModelo(post, tiposModel[i]);//pasarle aqui
				if(topicsAux.size()>=2){
					topicsFinal.add(topicsAux.get(0));
					if(topicsFinal.size()<2){
						topicsFinal.add(topicsAux.get(1));
					}
					return topicsFinal;
				}else if(topicsAux.size()>0){
					topicsFinal.add(topicsAux.get(0));
					if(topicsFinal.size()>=2){
						return topicsFinal;
					}
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return topicsFinal;
	}

	private List<String> busquedaModelo(Post post, String model) throws UnsupportedEncodingException, JSONException, IOException {
		post.addParameter("model", model);

		JSONObject jsonObj = new JSONObject(post.getResponse());
		JSONArray categorias = (JSONArray)jsonObj.get("category_list");
		List<String> topics = new ArrayList<String>();
		for(int i = 0; i<categorias.length()||topics.size()>=2; i++){
			if(topics.isEmpty()){
				topics.add(((JSONObject)categorias.get(i)).get("label").toString().split("-")[0]);
			}else if(!topics.get(0).equals(((JSONObject)categorias.get(i)).get("label").toString().split("-")[0])){
				topics.add(((JSONObject)categorias.get(i)).get("label").toString().split("-")[0]);
			}
		}
		return topics;
	}

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {

		ClassClient cc = new ClassClient();
		List<String> ja = cc.tematicaDataset("l01280796-salas-de-espectaculos-artisticos-teatros-cines-filmotecas-auditorios-y-salas-de-conciertos");
		System.out.println(ja.get(0));
		System.out.println(ja.get(1));


		// Show response
		/*System.out.println("Response");
      System.out.println("============");
      System.out.println(response);

      // Prints the specific fields in the response (categories)
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
      Document doc = docBuilder.parse(new ByteArrayInputStream(response.getBytes("UTF-8")));
      doc.getDocumentElement().normalize();
      Element response_node = doc.getDocumentElement();
      System.out.println("\nCategories:");
      System.out.println("==============");
      try {
        NodeList status_list = response_node.getElementsByTagName("status");
        Node status = status_list.item(0);
        NamedNodeMap attributes = status.getAttributes();
        Node code = attributes.item(0);
        if(!code.getTextContent().equals("0")) {
          System.out.println("Not found");
        } else {
          NodeList category_list = response_node.getElementsByTagName("category_list");
          if(category_list.getLength()>0){
            Node categories = category_list.item(0);          
            NodeList category = categories.getChildNodes();
            String output = "";
            for(int i=0; i<category.getLength(); i++) {
              Node info_category = category.item(i);  
              NodeList child_category = info_category.getChildNodes();
              String label = "";
              String code_cat = "";
              String relevance = "";
              for(int j=0; j<child_category.getLength(); j++){
                Node n = child_category.item(j);
                String name = n.getNodeName();
                if(name.equals("code"))
                  code_cat = n.getTextContent();
                else if(name.equals("label"))
                  label = n.getTextContent();
                else if(name.equals("relevance"))
                  relevance = n.getTextContent();
              }
              output += " + " + label + " (" +  code_cat + ")\n";
              output += "   -> relevance: " + relevance + "\n";
            }
            if(output.isEmpty())
              System.out.println("Not found");
            else
              System.out.print(output);
          }
        }
      } catch (Exception e) {
        System.out.println("Not found");
      }*/
	}
}
