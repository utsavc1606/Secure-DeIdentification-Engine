import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class FileToHdfs 
{
	public void callCommand()
	{
		System.out.println("Transferring the file to HDFS...");
		String s;
	    Process p;
	    Process q;
	    try
	    {
	    	p=Runtime.getRuntime().exec("hadoop fs -rm /user/cloudera/HiveDataFolder/OutputData.csv");
	    	p.waitFor();
	    	q=Runtime.getRuntime().exec("hadoop fs -copyFromLocal /home/cloudera/Desktop/datasets/OutputData.csv /user/cloudera/HiveDataFolder");
	      	BufferedReader br =new BufferedReader(new InputStreamReader(q.getInputStream()));
	    	while((s=br.readLine())!=null)
	    	{
	    	    System.out.println(s);
	    	    q.waitFor();
	    	    //System.out.println("exit: "+p.exitValue());
	    	    //p.destroy();
	    	}
	    	System.out.println("File Successfully transferred to HDFS...");
	    }
	    catch(Exception e)
	    {
	        System.out.println(e.getMessage());
	    }
	}
}
