import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class RefIDCreation 
{
	int seqRefId=1;
	public static List<String> dataList =new ArrayList(); 
    String OutputfilePath="/home/cloudera/Desktop/datasets/IDENTITYRESOLUTIONTEMP.csv";
    
    public void createRefIDs(String filePath)
    {
    	 System.out.println("Adding RefIDs to all the records in the Processed file......");
    	 RefIDCreation obj=new RefIDCreation();
	     obj.run(filePath);
	     obj.writeOutput();
	     System.out.println("RefIDs are created and the output File is: /home/cloudera/Desktop/datasets/IDENTITYRESOLUTIONTEMP.csv");
	     String s;
		 Process p;
		 Process q;
	     try
	     {
	    	 p=Runtime.getRuntime().exec("hadoop fs -rm /user/cloudera/HiveDataFolder/IDENTITYRESOLUTIONTEMP.csv");
	    	 p.waitFor();
	    	 q=Runtime.getRuntime().exec("hadoop fs -copyFromLocal /home/cloudera/Desktop/datasets/IDENTITYRESOLUTIONTEMP.csv /user/cloudera/HiveDataFolder");
	      	 BufferedReader br =new BufferedReader(new InputStreamReader(q.getInputStream()));
	    	 while((s=br.readLine())!=null)
	    	 {
	    	     System.out.println(s);
	    	     q.waitFor();
	    	 }
	    	 System.out.println("IDENTITYRESOLUTIONTEMP.csv file Successfully transferred to HDFS.....");
	     }
		 catch(Exception e)
		 {
		    System.out.println(e.getMessage());
		 }
    }
    public void run(String filePath)
	{
        
        BufferedReader br=null;
        String dataline="";
        String columnLine="";
        String firstname;
        String middlename;
        String lastname;
        String SSN;
        String DOB;
        String Remdata;
        int lengthof5;
        try
        {
            br = new BufferedReader(new FileReader(filePath));
            while((dataline=br.readLine())!=null)
            {
            	firstname=dataline.split("\\|")[0].replaceAll("[-+^,'!@#$%&*()_?/\\+=-]*","");
            	middlename=dataline.split("\\|")[1].replaceAll("[-+^,'!@#$%&*()_?/\\+=-]*","");
            	lastname=dataline.split("\\|")[2].replaceAll("[-+^,'!@#$%&*()_?/\\+=-]*","");
            	SSN=dataline.split("\\|")[3].replaceAll("[-+^,'!@#$%&*()_+=/\\-]*","");
            	DOB=dataline.split("\\|")[4].replaceAll("[-+^,'!@#$%&*()_+=-]*","");
            	//System.out.println(DOB);
            	//System.out.println(DOB.split("")[1]);
            	if(DOB.contains("/0"))
            	{
            		DOB=DOB.replace("/0", "/");
            	}
            	if(DOB.split("")[1].equals("0"))
            	{
            		//System.out.println(DOB);
            		DOB=DOB.substring(1, DOB.length());
            	}
            	//System.out.println(firstname+"|"+middlename+"|"+lastname+"|"+SSN+"|"+DOB);
            	lengthof5=(firstname+"|"+middlename+"|"+lastname+"|"+SSN+"|"+DOB+"|").length();
    			Remdata=dataline.substring(lengthof5+1,dataline.length());
            	dataList.add("Ref_"+seqRefId++ +"|"+firstname+"|"+middlename+"|"+lastname+"|"+SSN+"|"+DOB+"|"+Remdata);
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
	
	}
    public void writeOutput()
	{
		try
		{
			
			BufferedWriter bw=new BufferedWriter(new FileWriter(OutputfilePath));
			for (String s : dataList) 
			{
				//System.out.println(s);
			     bw.write(s);
			     bw.newLine();
			}
			bw.close();
			System.out.println("RefIDs are added and output file is created...");
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
