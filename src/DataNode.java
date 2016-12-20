import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
* Data node class  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class DataNode
{
	private static final int default_maxNodes = 32;
	private static final int default_replication_factor = 3;
	private static final double default_loadBal_Factor = 0.25;
		
	private int replicationFactor;	
		
	//max number of nodes possible in the ring
	private int maxNodes;
	private double loadBalFactor;
	
	//List of ringIds present in the ring
	List<Integer> nodesList = Collections.synchronizedList(new ArrayList<Integer>());
		
	//ringId as key and RingNode as value
	ConcurrentHashMap<Integer, DHTRingNode> nodesMap = new ConcurrentHashMap<Integer, DHTRingNode>();
	
	ConcurrentHashMap<Integer, String> ipMap = new ConcurrentHashMap<Integer, String>();
	ConcurrentHashMap<Integer, String> portMap = new ConcurrentHashMap<Integer, String>();
	
	//hash function to used for adding a key into the ring
	private Hashing hashFunction = new MurMurHashing();
	
	//random number generator
	Random rand = new Random();
	
	//other data nodes socket list
	List<Socket> dnSockList = Collections.synchronizedList(new ArrayList<Socket>());	
	
	ConcurrentHashMap<Integer, Socket> dnSockMap = new ConcurrentHashMap<Integer, Socket>();
	
	Socket ccSocket = null;
	
	/**
	 * Constructor
	 * @param size
	 */
	DataNode()
	{		
		ipMap.put(-1, "0");
	}
	
	/**
	 * display all the nodes in the ring
	 */
	synchronized void displayNodes()
	{
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
				System.out.println(key + " " + ipMap.get(key) + " " + portMap.get(key));
			}
		}
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
	}	
	
	/**
	 * get the index of the ringId's sorted in ascending order
	 * @param id
	 * @return
	 */
	int getRingIdIndex(int id)
	{
		synchronized(nodesList) 
		{
			for(int i=0;i<nodesList.size();++i)
			{
				if( nodesList.get(i) == id)
					return i;
			}
		}
		return -1;
	}
	
	/**
	 * compute the hash function for data
	 * @param str
	 * @return
	 */
	int getRingId(String str)
	{
		int hashcode = hashFunction.computeHash(str);
		int id = hashcode % maxNodes;
		return id;
	}
	
	/**
	 * This function deletes a node in the ring 
	 * @param id
	 */
	synchronized int delNode(int id)
	{
		if(!nodesList.contains(id))
		{
			System.out.format("Node ID %d doesn't exists in the ring... Delete a node operation failed\n", id);
			return -1;
		}
		
		int ind = getRingIdIndex(id);
		if(ind == -1)
		{
			System.out.format("Node ID %d not found in the ring\n", id);
			return -1;
		}
		
		if(nodesList.size() == 0)
		{
			System.out.println("No nodes.... Cant delete!!!!");
			return -1;
		}
		else if(nodesList.size() == 1)
		{
			synchronized(nodesList)
			{
				nodesList.clear();
			}
			
			synchronized(nodesMap)
			{
				nodesMap.remove(id);
			}
			
			synchronized(ipMap)
			{
				ipMap.remove(id);
				int vNum = Integer.parseInt(ipMap.get(-1));
				vNum++;
				ipMap.put(-1, String.valueOf(vNum));
			}
			synchronized(portMap)
			{
				portMap.remove(id);
			}
			synchronized(dnSockMap)
			{
				dnSockMap.remove(id);
			}
		}
		else
		{		
			synchronized(nodesList)
			{
				nodesList.remove(ind);
			}
			
			synchronized(nodesMap)
			{
				DHTRingNode r = nodesMap.get(id);				
			
				int pre = r.getPred();
				int suc = r.getSucc();
			
				DHTRingNode r1 = nodesMap.get(pre);
				r1.setSucc(suc);
				nodesMap.put(pre, r1);
			
				DHTRingNode r2 = nodesMap.get(suc);
			
				r2.setPred(pre);
				nodesMap.put(suc, r2);				
			
				nodesMap.remove(id);
			}
			
			synchronized(ipMap)
			{
				ipMap.remove(id);
				int vNum = Integer.parseInt(ipMap.get(-1));
				vNum++;
				ipMap.put(-1, String.valueOf(vNum));
			}
			synchronized(portMap)
			{
				portMap.remove(id);
			}
			synchronized(dnSockMap)
			{
				dnSockMap.remove(id);
			}
		}		
		return 0;
	}	
	
	/**
	 * This function will add a node in the ring
	 * @param nid
	 */
	synchronized int addNode(int nid, String ip, String port)
	{
		if(nodesList.contains(nid))
		{
			System.out.format("Node ID %d already exists in the ring... Add a node operation failed\n", nid);
			return -1;
		}
		synchronized(nodesList)
		{
			nodesList.add(nid);
			Collections.sort(nodesList);
		}
		
		DHTRingNode rn = new DHTRingNode(nid);
		
		if(nodesList.size() == 1)
		{
			synchronized(nodesMap)
			{
				nodesMap.put(rn.getrId(), rn);
			}
			synchronized(ipMap)
			{
				ipMap.put(rn.getrId(), ip);
				int vNum = Integer.parseInt(ipMap.get(-1));
				vNum++;
				ipMap.put(-1, String.valueOf(vNum));
			}
			synchronized(portMap)
			{
				portMap.put(rn.getrId(), port);
			}
		}
		else
		{
			int index = getRingIdIndex(rn.getrId());
			int predInd = -1;
			int sucInd = -1;
			int pre = -1;
			int suc = -1;
			synchronized(nodesList)
			{
				if(index == 0)
				{ 
					predInd = nodesList.size()-1;
					sucInd = (index + 1);
				}
				else if(index == nodesList.size()-1)
				{
					predInd = index - 1;
					sucInd = 0;
				}
				else
				{
					predInd = index - 1;
					sucInd = index + 1;
				}
				pre = nodesList.get(predInd);			
				suc = nodesList.get(sucInd);
			}
			
			synchronized(nodesMap)
			{
				DHTRingNode r1 = nodesMap.get(pre);			
				r1.setSucc(rn.getrId());
				nodesMap.put(pre, r1);
			
				DHTRingNode r2 = nodesMap.get(suc);
			
				r2.setPred(rn.getrId());			
				nodesMap.put(suc, r2);
			
				rn.setPred(pre);
				rn.setSucc(suc);			
			
				nodesMap.put(rn.getrId(), rn);
			}
			synchronized(ipMap)
			{
				ipMap.put(rn.getrId(), ip);
				int vNum = Integer.parseInt(ipMap.get(-1));
				vNum++;
				ipMap.put(-1, String.valueOf(vNum));
			}
			synchronized(portMap)
			{
				portMap.put(rn.getrId(), port);
			}
		}		
		return 0;
	}
	
	/**
	 * Performs load balance for the overloaded node identified by rId
	 * @param rId
	 */
	synchronized public String loadBalance(int rId)
	{
		DHTRingNode rn = nodesMap.get(rId);
		int pred = rn.getPred();
		int diff = 0;
		
		if(rId > pred)
			diff = rId - pred;
		else
			diff = maxNodes - pred + rId;
		
		String result = "";
		if(diff == 0)
		{
			System.out.format("Ring id %d 's predecessor is same as itself... Hence can't do load balancing\n", rId);
			return result;
		}
		else if(diff == 1)
		{
			System.out.format("Ring id %d and predecessor are seperated by one....Hence can't do load balancing\n", rId);
			return result;
		}
		else
		{
			int move = (int) Math.ceil(loadBalFactor * diff); 
			DHTRingNode balNode = nodesMap.get(pred);
			int newrId = pred + move;
			result = pred + "#" + newrId;
			balNode.setrId(newrId);
			DHTRingNode balPred = nodesMap.get(balNode.getPred());
			balPred.setSucc(newrId);
			
			DHTRingNode balSuc = nodesMap.get(balNode.getSucc());
			balSuc.setPred(newrId);
			
			synchronized(nodesMap)
			{
				nodesMap.remove(pred);
				nodesMap.put(newrId, balNode);
				
				//newly added
				nodesMap.put(balNode.getPred(), balPred);
				nodesMap.put(balNode.getSucc(), balSuc);
			}
			
			synchronized(nodesList)
			{
				nodesList.remove(getRingIdIndex(pred));
				nodesList.add(newrId);
				Collections.sort(nodesList);
			}
			
			synchronized(ipMap)
			{
				String val = ipMap.get(pred);
				ipMap.remove(pred);
				ipMap.put(newrId, val);
				
				int vNum = Integer.parseInt(ipMap.get(-1));
				vNum++;
				ipMap.put(-1, String.valueOf(vNum));
			}
						
			synchronized(portMap)
			{
				String val = portMap.get(pred);
				portMap.remove(pred);
				portMap.put(newrId, val);
			}
			
			synchronized(dnSockMap)
			{
				Socket val = dnSockMap.get(pred);
				if(val != null)
				{
					dnSockMap.remove(pred);
					dnSockMap.put(newrId, val);
				}
			}
			System.out.format("Node id %d has been unloaded by it's predecessor %d by %d keys\n", rId, pred, move);
			return result;
		}
	}
	
	/**
	 * checks if the rId holds the key either as the primary/secondary replica
	 * @param rid
	 * @param key
	 * @return
	 */
	synchronized public String lookupByRid(int rid, int key)
	{		
		int index = rid;
		String res = "";
		System.out.format("Look up for %d at %d\n", key, rid);
		for(int j=0;j<replicationFactor-1;++j)
		{
			DHTRingNode rn = null;
		
			synchronized(nodesMap)
			{
				rn = nodesMap.get(index);
			
				if(rn != null)
				{
					if(rn.isHolder(key))
					{						
						res += getAllReplicasByRid(rn.getrId());
						break;					
					}			
					index = rn.getPred();
				}
				else
				{
					System.out.println(" #### " + nodesMap);
				}
			}
		}
		return res;
	}
	
	/**
	 * This function looks up data in the ring
	 * @param data
	 */
	synchronized public void lookup(String data)
	{
		System.out.format("Looking up for %s\n", data);
		int id = getRingId(data);
		System.out.format("Hash code is %d\n", id);
		
		DHTRingNode r = nodesMap.get(id);
				
		if(r != null)
		{			
			System.out.format("$$$ Data can be found at %s\n",getAllReplicasByRid(r.getrId()));
			return;				
		}
		else
		{			
			for(Integer i : nodesMap.keySet())
			{
				int index = i;
				
				for(int j=0;j<replicationFactor-1;++j)
				{
					DHTRingNode rn = nodesMap.get(index);
					if(rn.isHolder(id) && j == 0)
					{						
						System.out.format("@@@ Server is the primary replica holder. Replicas at %s\n",getAllReplicasByRid(rn.getrId()));
						return;					
					}
					else if(rn.isHolder(id) && j > 0)
					{						
						System.out.format("@@@ Server is the secondary replica holder. Replicas at %s\n",getAllReplicasByRid(rn.getrId()));
						return;					
					}
					index = rn.getPred();
				}							
			}
		}		
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
	 * Gets load balance factor from the property file
	 * @return
	 */
	public static double getLoadBalFactor()
	{
		try
		{
			ReadPropFile inst;
			if((inst = ReadPropFile.getInstance()) != null)
			{
				String val =  inst.getLoadBalFactor();
				if(val != null)
					return Double.parseDouble(val);
			}			
		}
		catch(Exception e)
		{
			System.out.println("Exception thrown  :" + e);
		}		
		return default_loadBal_Factor;
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
		return default_replication_factor;
	}
	
	/**
	 * get all replicas where rId being the primary replica
	 * @param primaryRid
	 * @return
	 */
	String getAllReplicasByRid(int primaryRid)
	{			
		String res = "";
		ArrayList<Integer> aList = new ArrayList<Integer>();
		aList.add(primaryRid);
		int index = primaryRid;
		
		for(int i=0;i<replicationFactor-1;++i)
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
	
	/**
	 * main function
	 * @param args
	 */
	public static void main(String[] args)
	{		
		if (args.length != 1)
		{
	        System.err.println("Usage: java DataNode <port number>");
	        System.exit(1);
	    }

		int maxN = getMaxNodes();
		System.out.format("Max nodes possible in a ring is %d\n", maxN);
		DataNode dataNode = new DataNode();
		dataNode.maxNodes = getMaxNodes();
		dataNode.replicationFactor = getReplicationFactor();
		dataNode.loadBalFactor = getLoadBalFactor();
		
        int portNumber = Integer.parseInt(args[0]);
        boolean listening = true;
        
        try (ServerSocket serverSocket = new ServerSocket(portNumber))
        { 
            while (listening)
            {
	            new DataNodeThread(serverSocket.accept(), dataNode, false).start();	            
	        }
	    }
        catch(SocketException e)
        {        	
        	//System.err.println("Connection error");
        }
        catch (IOException e)
        {
            System.err.println("Could not listen on port " + portNumber);
            System.exit(-1);
        }
        
	}
}
