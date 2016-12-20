import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
* Control Client Class  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class ControlClient
{
	//Ring Id's as the list
	List<Integer> ridList = Collections.synchronizedList(new ArrayList<Integer>());
	
	//ring id as the key and IP address as value
	ConcurrentHashMap<Integer, String> ipMap = new ConcurrentHashMap<Integer, String>();
	//ring id as the key and port number as value
	ConcurrentHashMap<Integer, String> portMap = new ConcurrentHashMap<Integer, String>();
	
	//ring id as the key and socket as value
	ConcurrentHashMap<Integer, Socket> sockMap = new ConcurrentHashMap<Integer, Socket>();
	
	//ringId as key and RingNode as value
	ConcurrentHashMap<Integer, DHTRingNode> nodesMap = new ConcurrentHashMap<Integer, DHTRingNode>();
	
	Random rand = new Random();	
	long startTime;
	
	ControlClient()
	{
		ipMap.put(-1, "0");
	}
		
	/**
	 * displays the options for the user to enter
	 */
	public void displayOptions()
	{
		System.out.println("============================================================");
		System.out.println("The following operations are supported:");
		System.out.println("1. Enter 1 to Add a node");
		System.out.println("2. Enter 2 to Delete a node");
		System.out.println("3. Enter 3 to Load Balancing");	
		System.out.println("4. Enter 4 to print table");	
		System.out.println("99. Enter 99 to Exit");
		System.out.println("============================================================");			
	}
	
	/**
	 * display all the nodes in the ring
	 */
	synchronized void displayNodes()
	{		
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
	    	
	    	//control client establishes socket connection to the data node
	    	socConnToDNode(ipAddr, port, ringId);	    	
	    }	    
	    br.close();
	}

	/**
	 * establishes socket connection from client to newly added data node
	 * @param ipAddr
	 * @param port
	 * @param ringId
	 * @throws UnknownHostException
	 * @throws IOException
	 */
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
			DataOutputStream out = new DataOutputStream(client.getOutputStream());
			out.writeUTF("control_client");
				
			synchronized(sockMap)
			{
				sockMap.put(ringId, client);
			}
			new ControlClientThread(client, this).start();
		}
		catch(IOException e)
		{
			System.out.println("Couldn't establish socket connection");
		}
	}
	
	/**
	 * Sends a message to new data node to establish socket connections to every other data nodes
	 * @param rid
	 * @throws IOException
	 */
	private void sendConnectMessageToDNode(int rid) throws IOException
	{
		try
		{
			DataOutputStream out = new DataOutputStream(sockMap.get(rid).getOutputStream());
			String str = "";
		
			for(int j=0;j<ridList.size();++j)
			{
				int r = ridList.get(j);
				if(rid == r)
					continue;
		
				if(j < ridList.size()-1)
					str += (ipMap.get(r) + "_" + portMap.get(r) + "_" + r + "#");
				else
					str += (ipMap.get(r) + "_" + portMap.get(r)+ "_" + r);
			}	
		
			out.writeUTF("connect," + str);
		}
		catch(IOException e)
		{
			System.out.println("Socket not available");
		}
		catch(NullPointerException n)
		{
			System.out.println("Stream not available");
		}
	}
	
	/**
	 * Sends a message to establish socket connections from other data nodes to newly added node.
	 * @param rid
	 * @throws IOException
	 */
	
	private void sendConnMessFromDNodeToNNode(int rid) throws IOException
	{		
		DataOutputStream out;
		
		String str = ipMap.get(rid) + "_" + portMap.get(rid) + "_" + rid;
		
		for(int j=0;j<ridList.size();++j)
		{
			int r = ridList.get(j);
			if(rid == r)
				continue;			
						
			out = new DataOutputStream(sockMap.get(r).getOutputStream());
			
			out.writeUTF("connect," + str);
			//System.out.format("Connect message sent to %d as %s\n", r, str);
		}		
	}
	
	/**
	 * main function
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, SocketException
	{
		String configFileName = args[0];
		ControlClient cc = new ControlClient();
		
		long initStartTime = System.currentTimeMillis();
		//client will establish connection to all the data nodes as per the configuration file
		cc.readInitConfig(configFileName);		
		DataOutputStream out;		
				
		//each data node should establish socket connection to every other data nodes as per the configuration file				
		for(int i=0;i<cc.ridList.size();++i)
		{
			int rid = cc.ridList.get(i);
			cc.sendConnectMessageToDNode(rid);
		}		
		
		//randomly select one node and send add a node command as per the configuration file
		for(int i=0;i<cc.ridList.size();++i)
		{
			cc.rand = new Random();
			int index = cc.rand.nextInt(cc.ridList.size());
			
			int rid = cc.ridList.get(index);
			
			//System.out.format("Sent to rid %d\n", rid);			
			out = new DataOutputStream(cc.sockMap.get(rid).getOutputStream());
			cc.startTime = System.currentTimeMillis();
			out.writeUTF("add," + cc.ridList.get(i) + "," + cc.ipMap.get(cc.ridList.get(i)) + "," + cc.portMap.get(cc.ridList.get(i)));
			Thread.sleep(50);
		}
		long initEndTime = System.currentTimeMillis();
		double totalInitTime = (initEndTime - initStartTime);
		System.out.format("The total init time is %f milli seconds\n", totalInitTime);
		
		//infinite loop to read user input
		try
		{
			while(true)
			{	        
				cc.displayOptions();
				Scanner sc = new Scanner(System.in);
				int query = Integer.parseInt(sc.nextLine());
				int rid = -1;
				
				if(query == 1)
				{
					System.out.println("Enter the node Id to be added");
					int id = Integer.parseInt(sc.nextLine());
					
					if(id < 0)
					{
						System.out.println("Invalid node id... Retry!!!");
						continue;
					}
					
					System.out.println("Enter the IP address of data node");
					String ipAddr = sc.nextLine();
					
					if(ipAddr.equals("skip"))
					{
						continue;
					}
					
					System.out.println("Enter the listening port of the data node");
					int port = Integer.parseInt(sc.nextLine());
					
					if(port < 0)
					{
						System.out.println("Invalid port number... Retry!!!");
						continue;
					}
					
					if(cc.ipMap.containsKey(id))
					{
						System.out.println("Ring id's already exists..... Add node operation failed!!!");
						continue;
					}
					
					cc.startTime = System.currentTimeMillis();
					//control client to new data node socket connection
					cc.socConnToDNode(ipAddr, port, id);
					
					//new node will establish socket connection to all other nodes
					cc.sendConnectMessageToDNode(id);
					
					//all other data nodes will establish socket connection to new node
					cc.sendConnMessFromDNodeToNNode(id);					
					
					cc.rand = new Random();
					int index = cc.rand.nextInt(cc.ridList.size());
					rid = cc.ridList.get(index);
					while(rid == id)
					{
						cc.rand = new Random();
						index = cc.rand.nextInt(cc.ridList.size());
						rid = cc.ridList.get(index);
					}					
					out = new DataOutputStream(cc.sockMap.get(rid).getOutputStream());
					out.writeUTF("add," + id + "," + cc.ipMap.get(id) + "," + cc.portMap.get(id));
					Thread.sleep(100);
				}
				else if(query == 2)
				{
					System.out.println("Enter the node Id to be deleted");					
					int id = Integer.parseInt(sc.nextLine());	
					
					if(id < 0)
					{
						System.out.println("Invalid node id.... Retry!!!");
						continue;
					}
					
					cc.startTime = System.currentTimeMillis();
					int idx = cc.getRingIdIndex(id);
					if(idx != -1)
					{
						synchronized(cc.ridList)
						{
							cc.ridList.remove(idx);
						}
					
						synchronized(cc.ipMap)
						{
							cc.ipMap.remove(id);
						}
						synchronized(cc.portMap)
						{
							cc.portMap.remove(id);
						}
						synchronized(cc.sockMap)
						{
							cc.sockMap.remove(id);
						}
					
						cc.rand = new Random();
						int index = cc.rand.nextInt(cc.ridList.size());
						rid = cc.ridList.get(index);						
						out = new DataOutputStream(cc.sockMap.get(rid).getOutputStream());				
						out.writeUTF("delete," + id);
						Thread.sleep(100);
					}
					else
					{
						System.out.println("Node Id is not present in the ring!!!!!!! Failed");
					}
				}
				
				else if(query == 3)
				{
					System.out.println("Enter the node Id to be balanced");
					int id = Integer.parseInt(sc.nextLine());
					
					if(id < 0)
					{
						System.out.println("Invalid node id.... Retry!!!");
						continue;
					}
					cc.startTime = System.currentTimeMillis();
					
					int idx = cc.getRingIdIndex(id);
					if(idx == -1)
					{
						System.out.println("Node id is not present. Load balance operation failed!!!");
						continue;
					}
					
					cc.rand = new Random();
					int index = cc.rand.nextInt(cc.ridList.size());
					rid = cc.ridList.get(index);					
					out = new DataOutputStream(cc.sockMap.get(rid).getOutputStream());
					out.writeUTF("load_bal," + id);				
					Thread.sleep(100);				
				}
				
				else if(query == 4)
				{
					cc.displayNodes();
				}
				else if(query == 99)
				{
					System.out.println("Exiting!!!!!!!!");
					sc.close();
					System.exit(0);
				}
				else
				{
					System.out.println("Operation not supported");
				}
			}
		}
		catch(SocketException e)
        {
    		//System.err.println("Connection error");
        }
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
    	catch (IOException e)
    	{
    		e.printStackTrace();
    	}
	}	
}
