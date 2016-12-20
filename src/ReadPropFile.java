import java.io.*;
import java.util.*;

/**
* ReadPropFile reads configuration property file.  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class ReadPropFile
{
	protected static Properties prop = null;
			
	private static ReadPropFile instance = null;	
	
	protected ReadPropFile()	
	{		
		prop = new Properties();				
	}
	
	/**
	   * This method returns an instance of ReadPropFile 	
	   * @exception Exception If the configuration properties file read error
	   * @return Singleton object of ReadPropFile 	  
	 */
	public static ReadPropFile getInstance() throws Exception
	{
		try
		{
			if(instance == null)
			{
				instance = new ReadPropFile();
				
				/* FileInputStream throws an exception to avoid that first check if the file exists */
				File f = new File("conf.properties");
				if(f.exists())
				{
					FileInputStream fileStream = new FileInputStream("conf.properties");
					if(fileStream != null)
						prop.load(fileStream);
				}
				else
				{
					System.out.println("File not exists");
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Unable to read configuration properties file : " + e);			
		}
		return instance;		
	}
	
	/**
	   * This method reads nodeId_bit_size from the configuration properties file  	 
	   * @return bit size of nodeId 	  
	 */
	public String getMaxNodes()
	{
		return  prop.getProperty("maxNodes");		
	}
	
	public String getLoadBalFactor()
	{
		return  prop.getProperty("load_balance_factor");
	}
	
	public String getReplicationFactor()
	{
		return prop.getProperty("replication_factor");
	}
}