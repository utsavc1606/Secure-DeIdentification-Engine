import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.*;



public class MergeIndexFiles
{
	public void mergeFiles(int noOfIndices) throws IOException, InterruptedException
	{
		System.out.println("Index file merge program...");
		Process p;
		String s;
		BufferedReader br;
		String dir="/home/cloudera/Desktop/datasets/IndexFiles";
		
		System.out.println("Deleting all the exisitng file from local indexFile directory..");
		deleteLocalIndexFiles(dir);
		
		for(int i=1;i<=noOfIndices;i++)
		{
			p=Runtime.getRuntime().exec("hadoop fs -mv /user/cloudera/Index_"+i+"/part* /user/cloudera/Index_"+i+"/indexFile"+i);
			br=new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((s=br.readLine())!=null)
	    	{
	    	    System.out.println(s);
	    	    p.waitFor();
	    	}
			p.destroy();
			copyFilesToLocal(i);
		}
		mergeFilesOnLocal(dir,noOfIndices);
		MainIndexTableInput mti =new MainIndexTableInput();
    	mti.createMainIndexFile("/home/cloudera/Desktop/datasets/IndexFiles/MergedOne");
	}
	public void copyFilesToLocal(int i) throws InterruptedException
	{
		Process p;
		BufferedReader brb;
		String s;
		try {
			p=Runtime.getRuntime().exec("hadoop fs -copyToLocal /user/cloudera/Index_"+i+"/indexFile"+i+" /home/cloudera/Desktop/datasets/IndexFiles");
			brb=new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((s=brb.readLine())!=null)
	    	{
	    	    System.out.println(s);
	    	    p.waitFor();
	    	    //System.out.println("exit: "+p.exitValue());
	    	    //p.destroy();
	    	}
			p.destroy();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(i+" File Copied to local indexFile folder from HDFS..");
	}
	public void deleteLocalIndexFiles(String dir)
	{
		File path=new File(dir);
        for(File file:path.listFiles())
        {
        	file.delete();
        }
        System.out.println("Directory cleared for new files...");
	}
	public void mergeFilesOnLocal(String dir, int noOfIndices) throws InterruptedException
	{
		
		ArrayList<String> ls=new ArrayList();
	    ls.add("cat");
	    for(int i=1;i<=noOfIndices;i++)
	    {
	    	ls.add(dir+"/indexFile"+i+"");
	    }
			try
			{
				ProcessBuilder builder =new ProcessBuilder(ls);
				File combineFile =new File("/home/cloudera/Desktop/datasets/IndexFiles/MergedOne");
				builder.redirectOutput(combineFile);
				builder.redirectError(combineFile);
				Process p =builder.start();
				p.waitFor();
			}
			catch(IOException e)
			{
			    System.out.println(e.getMessage());	
			}
			System.out.println("Newly copied index files are Merged as one index file...");
	}
}

   
	

	


