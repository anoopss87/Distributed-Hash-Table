import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
* Regular Client Class  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class RegularClient
{
	List<Integer> ridList = Collections.synchronizedList(new ArrayList<Integer>());
	
	//ringId as key and RingNode as value
	ConcurrentHashMap<Integer, DHTRingNode> nodesMap = new ConcurrentHashMap<Integer, DHTRingNode>();
		
	ConcurrentHashMap<Integer, String> ipMap = new ConcurrentHashMap<Integer, String>();
	ConcurrentHashMap<Integer, String> portMap = new ConcurrentHashMap<Integer, String>();
	
	ConcurrentHashMap<String, Socket> sockMap = new ConcurrentHashMap<String, Socket>();
	Random rand = new Random();	
	
	long startTime;	
	private static final int default_maxNodes = 32;
	
	//hash function to used for adding a key into the ring
	private Hashing hashFunction = new MurMurHashing();
	
	RegularClient()
	{
		ipMap.put(-1, "0");
	}
	
	/**
	 * Get the bit size of the ring id by reading property file
	 * @return
	 */
	public static int getMaxNodes()
	{
		try
		{
			ReadPropFile inst;
			if((inst = ReadPropFile.getInstance()) != null)
			{
				String val =  inst.getMaxNodes();
				if(val != null)
					return Integer.parseInt(val);
			}			
		}
		catch(Exception e)
		{
			System.out.println("Exception thrown  :" + e);
		}		
		return default_maxNodes;
	}
	/**
	 * compute the hash function for data
	 * @param str
	 * @return
	 */
	int getRingId(String str)
	{
		int hashcode = hashFunction.computeHash(str);
		int maxN = getMaxNodes();
		int id = hashcode % maxN;
		return id;
	}
	
	/**
	 * displays the options for the user to enter
	 */
	public void displayOptions()
	{
		System.out.println("============================================================");
		System.out.println("The following operations are supported:");
		System.out.println("1. Enter 1 to Print table");
		System.out.println("2. Enter 2 to Lookup(read)");	
		System.out.println("3. Enter 3 to table update request");
		System.out.println("99. Enter 99 to Exit");
		System.out.println("============================================================");			
	}
	
	/**
	 * display all the nodes in the ring
	 */
	synchronized void displayNodes()
	{
		System.out.format("The version number of the local routing table is %d\n", Integer.parseInt(ipMap.get(-1)));
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		synchronized(nodesMap) 
		{
			for(int key : nodesMap.keySet())
			{
				System.out.println(key + " " + nodesMap.get(key).getPred() + " " + nodesMap.get(key).getSucc());
			}
		}
		
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		synchronized(ipMap) 
		{
			for(int key : ipMap.keySet())
			{
				if(key != -1)
					System.out.println(key + " " + ipMap.get(key) + " " + portMap.get(key));
			}
		}
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
	}
	
	/**
	 * Get the index of the given ring id
	 * @param id
	 * @return
	 */
	int getRingIdIndex(int id)
	{
		synchronized(ridList) 
		{
			for(int i=0;i<ridList.size();++i)
			{
				if( ridList.get(i) == id)
					return i;
			}
		}
		return -1;
	}
	
	/**
	 * initializes the data structures by reading configuration file
	 * @param fName
	 * @throws IOException
	 */
	public void readInitConfig(String fName) throws IOException
	{
		String filePath = System.getProperty("user.dir") + java.io.File.separator + fName;       
	    BufferedReader br = new BufferedReader(new FileReader(filePath));
	    
	    String line = "";
	    while((line = br.readLine()) != null)
	    {
	    	String[] words = line.split("\\s+");
	    	String ipAddr = words[0];
	    	int port = Integer.parseInt(words[1]);
	    	int ringId = Integer.parseInt(words[2]);
	    	socConnToDNode(ipAddr, port, ringId);	    	
	    }	    
	    br.close();
	}
	
	private void socConnToDNode(String ipAddr, int port, int ringId) throws UnknownHostException, IOException
	{
		synchronized(ridList)
		{
			ridList.add(ringId);
		}
		
		synchronized (ipMap)
		{		
			ipMap.put(ringId, ipAddr);
		}
		
		synchronized(portMap)
		{
			portMap.put(ringId, String.valueOf(port));
		}
		try
		{
			System.out.println("Connecting to " + ipAddr + " on port " + port);
			Socket client = new Socket(ipAddr, port);
			System.out.println("Just connected to " + client.getRemoteSocketAddress());
		
			synchronized(sockMap)
			{
				String key = ipAddr + "_" + port;
				sockMap.put(key, client);
			}
			new RegularClientThread(client, this).start();
		}		
		catch(IOException e)
		{
			System.out.println("Couldn't establish socket connection");
			System.out.println(e.getMessage());
		}		
	}
	
	String getAllReplicasByRid(int rId)
	{			
		String res = "";
		ArrayList<Integer> aList = new ArrayList<Integer>();
		aList.add(rId);
		int index = rId;
		int repFactor = getReplicationFactor() - 1;
		
		for(int i=0;i<repFactor;++i)
		{
			DHTRingNode r1 = nodesMap.get(index);
			aList.add(r1.getSucc());
			index = r1.getSucc();
		}
			
		int count = 0;
		
		for(Integer s : aList)
		{
			if(count < aList.size()-1)
				res += String.valueOf(s) + "#";
			else
				res += String.valueOf(s);
			count++;
		}
		return res;
	}
	
	public void lookup(String data) throws IOException
	{		
		int id = getRingId(data);		
		DataOutputStream out;
		boolean found = false;
		
		if(!nodesMap.isEmpty())
		{
			DHTRingNode r = nodesMap.get(id);
			System.out.format("Hash code is %d\n", id);	
			
			if(r != null)
			{
				found = true;
				System.out.format("Local look up found the data at ring id %d\n", r.getrId());
				System.out.format("Local replicas are %s\n", getAllReplicasByRid(r.getrId()));
				System.out.format("The version number of the local routing table is %d\n", Integer.parseInt(ipMap.get(-1)));
				String k = ipMap.get(r.getrId()) + "_" + portMap.get(r.getrId());
				Socket dnSoc = sockMap.get(k);	
				try
				{
					OutputStream outServer = dnSoc.getOutputStream();				
					out = new DataOutputStream(outServer);
					out.writeUTF("look_up," + r.getrId() + "," + id);
					return;
				}
				catch(IOException e)
				{
					sockMap.remove(k);
					int temp = getRingIdIndex(id);
					ridList.remove(temp);
					ipMap.remove(id);
					portMap.remove(id);
					nodesMap.remove(id);
					System.out.println("Primary data server not available..... Outdated table!!!!!!");
				}
			}
			else
			{			
				for(Integer i : nodesMap.keySet())
				{
					DHTRingNode rn = nodesMap.get(i);					
					if(rn.isHolder(id))
					{
						found = true;
						System.out.format("Local look up found the data at ring id %d\n", i);					
						System.out.format("Local replicas are %s\n", getAllReplicasByRid(rn.getrId()));
						System.out.format("The version number of the local routing table is %d\n", Integer.parseInt(ipMap.get(-1)));
						String k = ipMap.get(rn.getrId()) + "_" + portMap.get(rn.getrId());
						Socket dnSoc = sockMap.get(k);						
						try
						{
							OutputStream outServer = dnSoc.getOutputStream();				
							out = new DataOutputStream(outServer);
							out.writeUTF("look_up," + i + "," + id);
							return;
						}
						catch(IOException e)
						{
							sockMap.remove(k);
							int temp = getRingIdIndex(i);
							ridList.remove(temp);
							ipMap.remove(i);
							portMap.remove(i);
							nodesMap.remove(i);
							System.out.println("Primary data server not available..... Outdated table!!!!!!");
						}
					}
				}
				
				if(!found)
					System.out.println("Local table couldn't find data...");
				/*rand = new Random();
				int index = rand.nextInt(sockMap.size());
				int rid = ridList.get(index);
				outToServer = sockMap.get(rid).getOutputStream();
				out = new DataOutputStream(outToServer);				
				out.writeUTF("table_request");*/
			}
		}
		else
		{
			System.out.println("Local table is empty.... Do table update");
			/*System.out.format("The version number of the local routing table is %d\n", Integer.parseInt(ipMap.get(-1)));
			rand = new Random();
			int index = rand.nextInt(sockMap.size());	
			int rid = ridList.get(index);			
			out = new DataOutputStream(sockMap.get(rid).getOutputStream());				
			out.writeUTF("table_request");*/
		}
	}
	
	/**
	 * get replication factor by reading property file
	 * @return
	 */
	public static int getReplicationFactor()
	{
		try
		{
			ReadPropFile inst;
			if((inst = ReadPropFile.getInstance()) != null)
			{
				String val =  inst.getReplicationFactor();
				if(val != null)
					return Integer.parseInt(val);
			}			
		}
		catch(Exception e)
		{
			System.out.println("Exception thrown  :" + e);
		}		
		return 3;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		String configFileName = args[0];
		RegularClient rc = new RegularClient();
				
		//client will establish connection to all the data nodes as per the configuration file
		rc.readInitConfig(configFileName);
		
		//infinite loop to read user input
		while(true)
		{	        
			rc.displayOptions();
			Scanner sc = new Scanner(System.in);
			int query = Integer.parseInt(sc.nextLine());
			int rid = -1;			
			DataOutputStream out;
					
			if(query == 1)
			{
				rc.displayNodes();
			}
			else if(query == 2)
			{
				System.out.println("Enter the data to be looked up");
				String data = sc.nextLine();
				rc.startTime = System.currentTimeMillis();
				rc.lookup(data);
				Thread.sleep(100);
			}
			else if(query == 3)
			{
				rc.rand = new Random();
				int index = rc.rand.nextInt(rc.ridList.size());
				rid = rc.ridList.get(index);
				String k = rc.ipMap.get(rid) + "_" + rc.portMap.get(rid);
				try
				{					
					out = new DataOutputStream(rc.sockMap.get(k).getOutputStream());
					rc.startTime = System.currentTimeMillis();
					out.writeUTF("table_request");
					Thread.sleep(100);
				}
				catch(IOException e)
				{
					rc.sockMap.remove(k);
					int temp = rc.getRingIdIndex(rid);
					rc.ridList.remove(temp);
					rc.ipMap.remove(rid);
					rc.portMap.remove(rid);
					rc.nodesMap.remove(rid);
					System.out.println("Retry table updation.... Datanode was not available");
				}
			}
			else if(query == 99)
			{
				System.out.println("Exiting!!!!!!!!");
				sc.close();
				System.exit(0);
			}
		}		
	}
}
