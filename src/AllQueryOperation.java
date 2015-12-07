import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;


public class AllQueryOperation 
{
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public void performQueryOp(String filePath) throws SQLException, IOException
	{
		System.out.println("Running all intermediated database query and operations.....");
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
		 String sql ="TRUNCATE TABLE MAININDEXTABLE";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE TESTEXPLODE";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE NORMALIZEDMAININDEXTABLE";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE MAINTCOVERLAP";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE TCCANDIDATES";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE NONTCCANDIDATES";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE TCOUTPUT";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE PREFINALTCOP";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE FINALTCOP";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE FINALTCOPARRAY";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE CANDIDATEINDEX";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE NONCANDIDATEINDEX";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE IDENTITYRESOLUTIONTEMP";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE CANDIDATEDATANEW";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE PAIROUTPUT";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 sql =" TRUNCATE TABLE KNOWLEDGEBASE";
		 stmt.execute(sql);
		 System.out.println("Executing: "+sql);
		 

		 sql = "load data local inpath '" + filePath + "' into table MAININDEXTABLE";
		 System.out.println("Running: " + sql);
		 stmt.execute(sql);
		 
		 sql ="INSERT INTO TESTEXPLODE SELECT INDEXNAME,SID,SPLIT(INDEXSTRING,',') FROM MAININDEXTABLE";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql ="INSERT INTO NORMALIZEDMAININDEXTABLE SELECT INDEXNAME, SID, INDEXSTR FROM TESTEXPLODE LATERAL VIEW EXPLODE(INDEXSTRING) INDEXTBL AS INDEXSTR";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql ="INSERT INTO MAINTCOVERLAP SELECT INDEXSTRING1, SID1, SID2 FROM (SELECT A.INDEXSTRING AS INDEXSTRING1,B.INDEXSTRING AS INDEXSTRING2, A.SID AS SID1, B.SID AS SID2 FROM NORMALIZEDMAININDEXTABLE A,NORMALIZEDMAININDEXTABLE B WHERE A.INDEXSTRING=B.INDEXSTRING AND A.INDEXNAME IN ('Index_1', 'Index_2')) AS E WHERE E.SID1 <> E.SID2 AND E.INDEXSTRING1=E.INDEXSTRING2";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql ="INSERT INTO TCCANDIDATES SELECT INDEXNAME, SID, INDEXSTRING FROM MAININDEXTABLE WHERE MAININDEXTABLE.SID IN(SELECT SID1 AS SID3 FROM MAINTCOVERLAP UNION ALL SELECT SID2 AS SID3 FROM MAINTCOVERLAP)";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql="INSERT INTO NONTCCANDIDATES SELECT INDEXNAME, SID, INDEXSTRING FROM MAININDEXTABLE WHERE MAININDEXTABLE.SID NOT IN(SELECT SID1 AS SID3 FROM MAINTCOVERLAP UNION ALL SELECT SID2 AS SID3 FROM MAINTCOVERLAP)";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 TransitiveClosure tc =new TransitiveClosure();
		 tc.transitiveClosure(con);
		 
		 String s;
		 Process p;
		 Process q;
	     try
	     {
	    	 p=Runtime.getRuntime().exec("hadoop fs -rm /user/cloudera/HiveDataFolder/TCOUTPUT.csv");
	    	 p.waitFor();
	    	 q=Runtime.getRuntime().exec("hadoop fs -copyFromLocal /home/cloudera/Desktop/datasets/IndexFiles/TCOUTPUT /user/cloudera/HiveDataFolder");
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
		 
		 
		 sql ="LOAD DATA INPATH '/user/cloudera/HiveDataFolder/TCOUTPUT' into table TCOUTPUT";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql="INSERT INTO PREFINALTCOP SELECT SID, INDEXSTRING AS REFSTR FROM NONTCCANDIDATES UNION ALL SELECT SID, REFIDS AS REFSTR FROM TCOUTPUT";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql="INSERT INTO FINALTCOP SELECT DISTINCT * FROM PREFINALTCOP";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql="INSERT INTO FINALTCOPARRAY SELECT SID, SPLIT(REFIDS, ',') FROM FINALTCOP WHERE REFIDS LIKE '%,%'";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql="INSERT INTO CANDIDATEINDEX SELECT SID, SOURCEREF FROM FINALTCOPARRAY LATERAL VIEW EXPLODE(REFIDS) TCOPINDREF AS SOURCEREF";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql="INSERT INTO NONCANDIDATEINDEX SELECT * FROM FINALTCOP WHERE FINALTCOP.SID NOT IN (SELECT SID FROM CANDIDATEINDEX)";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 
		 sql="LOAD DATA INPATH '/user/cloudera/HiveDataFolder/IDENTITYRESOLUTIONTEMP.csv' into table IDENTITYRESOLUTIONTEMP";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 sql="INSERT INTO CANDIDATEDATANEW SELECT DISTINCT SID, REFID, FIRSTNAME, MIDDLENAME, LASTNAME, SSN, DOB, GENDER FROM CANDIDATEINDEX , IDENTITYRESOLUTIONTEMP WHERE TRIM(CANDIDATEINDEX.SOURCEID) =IDENTITYRESOLUTIONTEMP.REFID";
		 System.out.println("Running: "+sql);
		 stmt.execute(sql);
		 
		 System.out.println("Candidate data prepared for matching.....");
		 con.close();
		 
	}
}
