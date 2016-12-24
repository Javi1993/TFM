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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import org.bson.Document;
import org.json.JSONException;

/**
 * This class implements a starting client for Text Classification  
 */
public class ClassClient {

	public Set<String> tematicaDataset(String ID){
		// We define the variables needed to call the API
		String api = "http://api.meaningcloud.com/class-1.1";
		String key = "67d2d31e37c2ba1d032188b1233f19bf";

		Set<String> topicsFinal = new HashSet<String>() ;
		try{
			//String response = post.getResponse();
			String[] tiposModel = new String[]{"IPTC_es","SocialMedia_es","EUROVOC_es_ca"};//checkear 2º y 3º
			for(int i = 0; i<tiposModel.length; i++){
				Post post = new Post (api);
				post.addParameter("key", key);
				post.addParameter("txt", ID);
				post.addParameter("of", "json");
				Set<String> topicsAux = busquedaModelo(post, tiposModel[i], ID);//pasarle aqui
				if(topicsAux!=null && !topicsAux.isEmpty()){
					topicsFinal.addAll(topicsAux);
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return topicsFinal;
	}

	@SuppressWarnings("unchecked")
	private Set<String> busquedaModelo(Post post, String model, String ID) throws UnsupportedEncodingException, JSONException, IOException {
		post.addParameter("model", model);
		Document JSON = null;
		try{
			byte[] encoded = post.getResponse().getBytes();
			JSON = Document.parse(new String(encoded, "UTF-8"));
			List<Document> categorias = (List<Document>) JSON.get("category_list");
			Set<String> topics = new HashSet<String>();
			for(Document cat:categorias){
				String valor = cat.get("label").toString().split("-")[0].trim();
				if(model.equals("EUROVOC_es_ca")){
					valor = valor.split("/")[1].replaceAll("[(es)]", "").trim();
				}
				topics.add(valor);
			}
			Thread.sleep(500);
			return topics;
		}catch (Exception e) {
			System.err.println("No hay valores para el modelo "+model+" de la ID "+ID+".");
			System.err.println(JSON.toJson());
			return null;
		}
	}

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {

		ClassClient cc = new ClassClient();
		Set<String> ja = cc.tematicaDataset("l01280796-centros-para-personas-sin-hogar");
		System.out.println(ja.size());
//		System.out.println(ja.get(0));
//		System.out.println(ja.get(1));


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
