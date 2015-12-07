import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

public class MainProgram {

	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, InterruptedException, SQLException 
	{
        Calendar cal1=Calendar.getInstance();
        SimpleDateFormat sdf1 =new SimpleDateFormat("HH:mm:ss");
		String startTime =sdf1.format(cal1.getTime());
		
		System.out.println("Program execution started................");
		
		String InputfilePath="/home/cloudera/Desktop/datasets/SAChildCount_0809-1415.csv";
		ProcessInputFile pif=new ProcessInputFile();
		pif.processInputFile(InputfilePath);
		
		
		RefIDCreation refIdcreation =new RefIDCreation();
		refIdcreation.createRefIDs("/home/cloudera/Desktop/datasets/ProcessedInputFile.csv");
		
		System.out.println("Reading Index Descriptor xml.....");
	    XmlReaderClass xmlrdr =new XmlReaderClass();
	    HashMap<String, String> indexMap= xmlrdr.xmlRead("/home/cloudera/Desktop/datasets/indexDescriptor.xml");
	    System.out.println("Indices are as follows.....");
	    for(Entry<String, String> map: indexMap.entrySet())
	    {
	    	System.out.println(map.getKey()+"    "+map.getValue());
	    }
	    String indexCols="";
	    String s;
	    Process p;
	    for(Entry<String, String> hs: indexMap.entrySet())
	    {
	    	if(hs.getValue().toLowerCase().contains(("firstname")))
	    	{
                indexCols+=1+",";	    		
	    	}
	    	if(hs.getValue().toLowerCase().contains("lastname"))
	    	{
	    	    indexCols+=3+",";	
	    	}
	    	if(hs.getValue().toLowerCase().contains("ssn"))
	    	{
	    	    indexCols+=4+",";	
	    	}
	    	if(hs.getValue().toLowerCase().contains("dob"))
	    	{
	    	    indexCols+=5+",";	
	    	}
	    	
	    	
	    	System.out.println("Running Indexing Map Reduce with index as :"+indexCols.substring(0, indexCols.length()-1));
	    	
	    	p=Runtime.getRuntime().exec("hadoop jar /home/cloudera/Desktop/executable_jars/IndexTest_v1.jar IndexTest /user/cloudera/HiveDataFolder/IDENTITYRESOLUTIONTEMP.csv /user/cloudera/"+hs.getKey()+" "+indexCols.substring(0, indexCols.length()-1)+" "+hs.getKey());
	    	p.waitFor();
	    	BufferedReader br =new BufferedReader(new InputStreamReader(p.getInputStream()));
	    	
	    	while((s=br.readLine())!=null)
	    	{
	    	    System.out.println(s);
	    	}
	    	indexCols="";
	    	System.out.println("IndexOutputs on HDFS" + "/user/cloudera/"+hs.getKey()+"/part-r-00000"+" created...");
	    }
	    
	    int noOfIndex=indexMap.size();
    	MergeIndexFiles mif=new MergeIndexFiles();
    	mif.mergeFiles(noOfIndex);
    	System.out.println("Input for MainIndexTable is Created...");
    	
    	
    	String filePath="/home/cloudera/Desktop/datasets/IndexFiles/MainIndexTabInput";
    	AllQueryOperation aqo=new AllQueryOperation();    	
    	aqo.performQueryOp(filePath);
    	
    	PairWiseMatching pwm=new PairWiseMatching();
    	pwm.pairWiseMatching();
    	
    	Calendar cal2=Calendar.getInstance();
        SimpleDateFormat sdf2 =new SimpleDateFormat("HH:mm:ss");
 		String endTime =sdf2.format(cal2.getTime());
 		
    	System.out.println("Start time: "+startTime);
    	System.out.println("End Time: "+endTime);
	    

	}

}
