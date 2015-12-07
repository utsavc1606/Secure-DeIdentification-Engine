import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.xml.sax.SAXException;


public class PairWiseMatching 
{
	RuleReaderClass xmlrdr =new RuleReaderClass();
    HashMap<String, String> rulesMap;
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public void pairWiseMatching() throws ParserConfigurationException, SAXException, IOException, SQLException
    {
    	System.out.println("Pairwise matching of records program.....");
    	rulesMap = xmlrdr.ruleRead("/home/cloudera/Desktop/datasets/ruleDescriptor.xml");
    	System.out.println("The rules are as follows...");
    	for(Entry<String, String> map: rulesMap.entrySet())
	    {
	    	System.out.println(map.getKey()+"    "+map.getValue());
	    }
    	Map<String, String> candidates=new HashMap<String,String>();
    	Map<String, String[]> data=new HashMap<String, String[]>();
    	Map<String, String> matchOP=new HashMap<String, String>();
    	String candidatesKey;
        String candidatesVal;
        
        try 
		 {
		      Class.forName(driverName);
		 }
		 catch (ClassNotFoundException e) 
		 {
		      e.printStackTrace();
		      System.exit(1);
		 }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
		Statement stmt = con.createStatement();
        String CandidatesQuery = "select * from finalTCOP where RefIDs like '%,%'";
        ResultSet res=stmt.executeQuery(CandidatesQuery);
        while(res.next())
        {
        	candidatesKey = res.getString(1);
            candidatesVal = res.getString(2);
            if (!(candidates.containsKey(candidatesKey)))
            {
                candidates.put(candidatesKey, candidatesVal);
                //System.out.println("Candidate Data: "+candidatesKey+"  "+ candidatesVal);
            }
			
		}
        
        String dataKey;
        String dataVal;

        String dataQuery = "select * from CandidateDataNew";
        res=stmt.executeQuery(dataQuery);
        while(res.next())
        {
        	dataKey = res.getString(2);
            dataVal = res.getString(3) + "|" + res.getString(4) + "|" + res.getString(5) + "|" + res.getString(6) + "|" + res.getString(7) + "|" + res.getString(8);
            data.put(dataKey, dataVal.split("|"));
            //System.out.println("Data Map data: "+dataKey+"  "+dataVal);
        }
        
        String[] refids;
        int trueCounter = 0;
        String UID;
        int col1=0;
        int col2=0;
        int col3=0;
        
        try
        {
	        for(Entry<String, String> cand: candidates.entrySet())
	        {
	        	refids = cand.getValue().split(",");
	        	for (int i = 0; i < refids.length; i++)
	            {
	                for (int j = i + 1; j < refids.length; j++)
	                {
	                	for (Entry<String,String> rule:rulesMap.entrySet())
	                	{
	                		 if (StringUtils.countMatches(rule.getValue(),"|") == 2)
	                         {
	                             if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("firstname"))
	                                 col1 = 0;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("lastname"))
	                                 col1 = 2;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("ssn"))
	                                 col1 = 3;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("dob"))
	                                 col1 = 4;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("gender"))
	                                 col1=5;
	
	                             if(rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("firstname"))
	                                 col2=0;
	                             else if(rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("lastname"))
	                                 col2=2;
	                             else if (rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("ssn"))
	                                 col2 = 3;
	                             else if(rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("dob"))
	                                 col2=4;
	                             else if(rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("gender"))
	                                 col2=5;
	
	                             if(rule.getValue().split("|")[2].split(",")[0].toLowerCase().equals("firstname"))
	                                 col3=0;
	                             else if(rule.getValue().split("|")[2].split(",")[0].toLowerCase().equals("lastname"))
	                                 col3=2;
	                             else if (rule.getValue().split("|")[2].split(",")[0].toLowerCase().equals("ssn"))
	                                 col3 = 3;
	                             else if(rule.getValue().split("|")[2].split(",")[0].toLowerCase().equals("dob"))
	                                 col3=4;
	                             else if(rule.getValue().split("|")[2].split(",")[0].toLowerCase().equals("gender"))
	                                 col3=5;
	
	                             if (data.get(refids[i].trim())[col1].equals(data.get(refids[j].trim())[col1]) && data.get(refids[i].trim())[col2].equals(data.get(refids[j].trim())[col2]) && data.get(refids[i].trim())[col3].equals(data.get(refids[j].trim())[col3]))
	                                 {
	                                  trueCounter++;
	                                 }
	                         }
	                         else if (StringUtils.countMatches(rule.getValue(),"|") == 1)
	                         {
	                        	 
	                             if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("firstname"))
	                                 col1 = 0;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("lastname"))
	                                 col1 = 2;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("ssn"))
	                                 col1 = 3;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("dob"))
	                                 col1 = 4;
	                             else if (rule.getValue().split("|")[0].split(",")[0].toLowerCase().equals("gender"))
	                                 col1 = 5;
	
	                             if (rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("firstname"))
	                                 col2 = 0;
	                             else if (rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("lastname"))
	                                 col2 = 2;
	                             else if (rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("ssn"))
	                                 col2 = 3;
	                             else if (rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("dob"))
	                                 col2 = 4;
	                             else if (rule.getValue().split("|")[1].split(",")[0].toLowerCase().equals("gender"))
	                                 col2 = 5;
	                             
	                             if (data.get(refids[i].trim())[col1].equals(data.get(refids[j].trim())[col1]) && data.get(refids[i].trim())[col2].equals(data.get(refids[j].trim())[col2]))
	                             {
	                                 trueCounter++;
	                             }
	                         }
	                    }
	                    if (trueCounter >= 2)
	                    {
	                        UID = randGenerator();
	                        if (!(matchOP.containsKey(refids[i])) && !(matchOP.containsKey(refids[j])))
	                        {
	                            matchOP.put(refids[i], UID);
	                            matchOP.put(refids[j], UID);
	                        }
	                        else if (!(matchOP.containsKey(refids[j])))
	                        {
	                            matchOP.put(refids[j], matchOP.get(refids[i]));
	                            // System.out.println(refids[j]+" "+matchOP.get(refids[i]));
	                        }
	                    }
	                    trueCounter = 0;
	                }
	            }
	        }
        }
        catch(Exception ex)
        {
        	System.out.println("******Something is wrong during matching*******");
        }
        
        String sql="Select * from noncandidateindex";
        ResultSet rs=stmt.executeQuery(sql);
        while(rs.next())
        {
        	matchOP.put(rs.getString(2),randGenerator());
        }
       
        BufferedWriter bw=new BufferedWriter(new FileWriter("/home/cloudera/Desktop/datasets/IndexFiles/PairOutput"));
        System.out.println("Writing output to a file...");
        for(Entry<String, String> pair: matchOP.entrySet())
        {
        	bw.write(pair.getValue()+"|"+pair.getKey().trim());
        	bw.newLine();
        }
        bw.close();
        
        sql="LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/datasets/IndexFiles/PairOutput' into table PAIROUTPUT";
        System.out.println("Running: "+sql);
        stmt.execute(sql);
        
        
        sql="INSERT INTO KNOWLEDGEBASE SELECT RID, pairoutput.Refid, firstname, middlename, lastname, ssn,dob, gender, remdata from pairoutput, identityresolutiontemp where pairoutput.refid=identityresolutiontemp.refid";
        System.out.println("Running: "+sql);
        stmt.execute(sql);
        
        System.out.println("Matching completed and records are stored in Knowledgebase table with RIDs.....");
    }
    public static String randGenerator()
	{
		int length=10;
		boolean useLetters=true;
		boolean useNumbers=true;
		String generatedString= RandomStringUtils.random(length, useLetters, useNumbers);
		return generatedString.toUpperCase();
	}
    
    
}
