import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.RandomStringUtils;


public class MainIndexTableInput 
{   
	
	public void createMainIndexFile(String path) throws IOException, FileNotFoundException
	{
		System.out.println("Inside mainindextableinput program...");
		Map<String,String> map=createMap(path);
		
	    
	    BufferedWriter bw=new BufferedWriter(new FileWriter("/home/cloudera/Desktop/datasets/IndexFiles/MainIndexTabInput"));
	    System.out.println("Printing the map....");
	    if(!(map.isEmpty()))
	    {
	    for(Entry<String, String> mp: map.entrySet())
		    {
	    	    //System.out.println("printing...");
		    	//System.out.println(mp.getKey().substring(0, mp.getKey().indexOf('|'))+"|"+mp.getValue());
		    	bw.write(mp.getKey().substring(0, mp.getKey().indexOf('|'))+"|"+mp.getValue()+"\n");
		    	
		    }
	    }
	    else
	    {
	    	System.out.println("Map is empty......");
	    }
	    
	    System.out.println("Back to MergedIndex File program....");
	    bw.close();
	    
	}
	public Map createMap(String path) throws IOException
	{
		String lines;
		String key;
		String value;
		Map<String, String> map=new HashMap<String, String>();
		System.out.println(path);
	    BufferedReader br=new BufferedReader(new FileReader(path));
	    while((lines=br.readLine())!=null)
	    {
	    	//System.out.println(lines);
	    	key=lines.split("\t")[0];
	    	value=lines.split("\t")[1];
	    	//System.out.println(key+"|"+value);
	    	map.put(key, value);
	    	//System.out.println("Creating the map.....");
	    }
	    
	    br.close();
	    return map;
	}
	

}
