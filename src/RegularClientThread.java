import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;

/**
* Regular Client thread class.  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class RegularClientThread extends Thread
{
	private Socket socket = null;
	private RegularClient rClient = null;
	
	/**
	 * Constructor
	 * @param socket
	 * @param rc
	 */
	RegularClientThread(Socket socket, RegularClient rc)
	{
		super("RegularClientThread");
        this.socket = socket;
        this.rClient = rc;
	}
	
	 /**
     * Converts string to hash map
     * @param str
     */
    public void strTonodesMap(String str)
    {    	
    	String[] entry = str.split("#");
    	
    	synchronized (rClient.nodesMap)
    	{		
    		rClient.nodesMap.clear();
    		for(String kv : entry)
    		{
    			String[] t = kv.split("_");
    			int r = Integer.parseInt(t[0]);
    			int p = Integer.parseInt(t[1]);
    			int s = Integer.parseInt(t[2]);
    			DHTRingNode rn = new DHTRingNode(r, p, s);
    			rClient.nodesMap.put(r, rn);
    		}
    	}
    }
    
    /**
     * Converts string to hash map
     * @param str
     * @throws IOException 
     * @throws UnknownHostException 
     * @throws NumberFormatException 
     */
    synchronized public void strToipPortMap(String str) throws NumberFormatException, UnknownHostException, IOException
    {    	
    	String[] entry = str.split("#");
    	
    	ConcurrentHashMap<Integer, String> ipOld = new ConcurrentHashMap<Integer, String>();
    	ConcurrentHashMap<Integer, String> portOld = new ConcurrentHashMap<Integer, String>();
    	
    	ConcurrentHashMap<Integer, String> ipDiff = new ConcurrentHashMap<Integer, String>();
    	ConcurrentHashMap<Integer, String> portDiff = new ConcurrentHashMap<Integer, String>();    	
    	
    	synchronized (rClient.portMap)
    	{
    		synchronized(rClient.ipMap)
    		{
    			ipOld.putAll(rClient.ipMap);
    			portOld.putAll(rClient.portMap);
    			rClient.ipMap.clear();
    			rClient.portMap.clear();
    			for(String kv : entry)
    			{
    				String[] t = kv.split("_");
    				int rid = Integer.parseInt(t[0]);
    				rClient.ipMap.put(rid, t[1]);
    				rClient.portMap.put(rid, t[2]);
    				ipDiff.put(rid, t[1]);
    				portDiff.put(rid, t[2]);
    			}
    		}
    	}
    	
    	/* Establish socket connection to the new data nodes 
    	 * which was received during table update request */
    	ipDiff.entrySet().removeAll(ipOld.entrySet());
    	portDiff.entrySet().removeAll(portOld.entrySet());
    	
    	synchronized(rClient.ipMap)
    	{
    		synchronized(rClient.portMap)
    		{    	
    			ipOld.entrySet().removeAll(rClient.ipMap.entrySet());
    			portOld.entrySet().removeAll(rClient.portMap.entrySet());
    		}
    	}
    	
    	for(Integer key : ipDiff.keySet())
    	{
    		for(Integer k : ipOld.keySet())
    		{
    			if(ipOld.get(k).equals(ipDiff.get(key)) && portOld.get(k).equals(portDiff.get(key)))
    			{
    				ipDiff.remove(key);
    				break;
    			}
    		}
    	}
    	
    	for(int rid : ipDiff.keySet())
    	{
    		if(rid != -1)
    		{
    			System.out.println("Connecting to " + ipDiff.get(rid) + " on port " + portDiff.get(rid));
    			Socket client = new Socket(ipDiff.get(rid), Integer.parseInt(portDiff.get(rid)));
    			System.out.println("Just connected to " + client.getRemoteSocketAddress());
    		
    			synchronized(rClient.sockMap)
    			{
    				String k = ipDiff.get(rid) + "_" + portDiff.get(rid);
    				rClient.sockMap.put(k, client);
    			}
    			new RegularClientThread(client, rClient).start();
    		}
    	}
    }
    
    /**
     * Converts string to list
     * @param str
     */
    public void strTonodesList(String str)
    {    	
    	String[] entry = str.split("#");
    	
    	synchronized (rClient.ridList)
    	{		
    		rClient.ridList.clear();
    		for(String v : entry)
    		{    			
    			int r = Integer.parseInt(v);    			
    			rClient.ridList.add(r);
    		}
    	}
    }
	
	 /**
     * Thread run function
     */
    public void run()
    {    	
    	try
    	{
    		DataInputStream in = new DataInputStream(socket.getInputStream());
    		double estTime = 0.0;
			//DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    		while(true)
        	{
    			String data = in.readUTF();		    				                
				String[] arg = data.split(",");    
				
				switch(arg[0])
				{
					case "update_map":
						strTonodesMap(arg[1]);
						break;
						
					case "update_list":
						strTonodesList(arg[1]);
						break;
						
					case "update_ipPortMap":
						strToipPortMap(arg[1]);
						break;					
						
					case "table_update_complete":
						System.out.println("Table updation completed");
						estTime = (System.currentTimeMillis() - rClient.startTime);
						System.out.format("The total time for table updation is %f milli seconds\n", estTime);
						break;
						
					case "Not_Found":
						System.out.println("Data not found at the requested datanode....");
						System.out.format("The version number of the server routing table is %s\n", arg[1]);						
						estTime = (System.currentTimeMillis() - rClient.startTime);
						System.out.format("The total look up time is %f milli seconds\n", estTime);
						//out.writeUTF("table_request");
						break;
					
					case "Found":
						System.out.format("The replicas for the data are %s\n", arg[2]);
						System.out.format("The version number of the server routing table is %s\n", arg[1]);
						estTime = (System.currentTimeMillis() - rClient.startTime);
						System.out.format("The total look up time is %f milli seconds\n", estTime);
						break;
				}
        	}
    	}
    	catch(SocketException e)
        {
        	//System.err.println("Connection error");
        }
    	catch (EOFException e)
    	{
    		// ... this is fine
    	}
		catch (IOException e)
		{
			e.printStackTrace();
		}
    }
}
