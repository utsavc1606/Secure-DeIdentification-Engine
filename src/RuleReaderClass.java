import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class RuleReaderClass 
{
	public HashMap<String, String> ruleRead(String rulePath) throws ParserConfigurationException, SAXException, IOException
	{
		System.out.println("Inside rule reader");
		HashMap<String, String> ruleMap =new HashMap();
		File file = new File(rulePath);
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document document = documentBuilder.parse(file);
		NodeList nodelist = document.getDocumentElement().getChildNodes();   //getElementsByTagName("Index");
		String key="";
		String value="";
        for(int i=0;i < nodelist.getLength();i++)
        {
        	
        	if(nodelist.item(i).hasAttributes())
        	{
        	   
        	   NodeList childnodeList= nodelist.item(i).getChildNodes();
        	   for(int j=0;j<childnodeList.getLength();j++)
        	   {   
        		   
        	       if(childnodeList.item(j).hasAttributes()) 
        	       {
        	       	key=nodelist.item(i).getAttributes().getNamedItem("id").getNodeValue();
        	       	value= childnodeList.item(j).getAttributes().getNamedItem("name").getNodeValue()+","+childnodeList.item(j).getAttributes().getNamedItem("noofChars").getNodeValue();
        	       	if(ruleMap.containsKey(key))
        	       	{
        	       		
        	       		ruleMap.put(key, ruleMap.get(key)+"|"+value); 
        	       	}
        	       	else
        	       	{
        	       		
        	       		ruleMap.put(key, value);
        	       	}
        	       	
        	       }
        	   }
        	}
        }
        System.out.println("Xml reader completed, returning the map...");
		return ruleMap;

		
	}


}
