import java.io.*;
import java.net.*;

/**
* Data node thread class.  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class DataNodeThread extends Thread
{
	private Socket socket = null;
	private DataNode dNode = null;	
	
	/**
	 * Constructor
	 * @param socket
	 * @param dn
	 */
    public DataNodeThread(Socket socket, DataNode dn, boolean server)
    {
        super("DataNodeThread");
        this.socket = socket;
        this.dNode = dn;               
    }
    
    /**
     * Converts hash map to string
     * @return
     */
    synchronized public String nodesMapToStr()
    {
    	String str = "";
    	int count = 0;
    	
    	synchronized(dNode.nodesMap)
    	{
	    	int size = dNode.nodesMap.size();
	    	for(int r : dNode.nodesMap.keySet())
	    	{
	    		DHTRingNode temp = dNode.nodesMap.get(r);
	    		if(count < size-1)
	    			str += (String.valueOf(r) + "_" + temp.getPred() + "_" + temp.getSucc() + "#");
	    		else
	    			str += (String.valueOf(r) + "_" + temp.getPred() + "_" + temp.getSucc());
	    		count++;
	    	}
    	}
    	return str;
    }
    
    /**
     * Converts string to hash map
     * @param str
     */
    synchronized public void strTonodesMap(String str)
    {    	
    	String[] entry = str.split("#");
    	
    	synchronized (dNode.nodesMap)
    	{		
    		dNode.nodesMap.clear();
    		for(String kv : entry)
    		{
    			String[] t = kv.split("_");
    			int r = Integer.parseInt(t[0]);
    			int p = Integer.parseInt(t[1]);
    			int s = Integer.parseInt(t[2]);
    			DHTRingNode rn = new DHTRingNode(r, p, s);
    			dNode.nodesMap.put(r, rn);
    		}
    	}
    }
    
    /**
     * Converts hash map to string
     * @return
     */
    synchronized public String ipPortMapToStr()
    {
    	String str = "";
    	int count = 0;
    	
    	synchronized(dNode.ipMap)
    	{
    		int size = dNode.ipMap.size();
    		for(int r : dNode.ipMap.keySet())
    		{
    			String ip = dNode.ipMap.get(r);
    			String port = dNode.portMap.get(r);
    			if(count < size-1)
    				str += (String.valueOf(r) + "_" + ip + "_" + port + "#");
    			else
    				str += (String.valueOf(r) + "_" + ip + "_" + port);
    			count++;
    		}
    	}
    	return str;
    }
    
    /**
     * Converts string to hash map
     * @param str
     */
    synchronized public void strToipPortMap(String str)
    {    	
    	String[] entry = str.split("#");
    	
    	synchronized (dNode.ipMap)
    	{
    		synchronized(dNode.portMap)
    		{
    			dNode.ipMap.clear();
    			dNode.portMap.clear();
    			for(String kv : entry)
    			{
    				String[] t = kv.split("_");
    				int rid = Integer.parseInt(t[0]);
    				dNode.ipMap.put(rid, t[1]);
    				dNode.portMap.put(rid, t[2]);
    			}
    		}
    	}
    }
    
    /**
     * Converts list to string
     * @return
     */
    synchronized public String nodesListToStr()
    {
    	String str = "";
    	int count = 0;
    	
    	synchronized(dNode.nodesList)
    	{
    		int size = dNode.nodesList.size();
    		for(int r : dNode.nodesList)
    		{    		
    			if(count < size-1)
    				str += (String.valueOf(r) + "#");
    			else
    				str += (String.valueOf(r));
    			count++;
    		}
    	}
    	return str;
    }
    
    /**
     * Converts string to list
     * @param str
     */
    synchronized public void strTonodesList(String str)
    {    	
    	String[] entry = str.split("#");
    	
    	synchronized (dNode.nodesList)
    	{		
    		dNode.nodesList.clear();
    		for(String v : entry)
    		{    			
    			int r = Integer.parseInt(v);    			
    			dNode.nodesList.add(r);
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
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    		while(true)
        	{
    			String data = in.readUTF();    			
    				                
				String[] arg = data.split(",");    
				String val = "";
				String val2 = "";
				String val3 = "";
				String val4 = "";
				String reply = "reply,";
				int ret = -1;
				switch(arg[0])
				{
					case "add":						
						//call local addNode method using dataNode object
						ret = dNode.addNode(Integer.parseInt(arg[1]), arg[2], arg[3]);
						
						//updated nodesMap has to be sent to other data nodes.
						if(ret == -1)
						{
							System.out.println("Node add operation failed!!!!!!!!!");
							reply += "Node add operation failed!!!!!!!!!";							
						}
						else
						{
							val = "update_map,";
							val2 = "update_list,";
							val3 = "update_ipPortMap,";
							
							//convert hash map to string
							val += nodesMapToStr();
							val2 += nodesListToStr();
							val3 += ipPortMapToStr();
													
							//send nodesMap in string to all other data nodes.
							synchronized(dNode.dnSockMap)
							{
								for(Integer r : dNode.dnSockMap.keySet())
								{									
									Socket soc = dNode.dnSockMap.get(r);
									DataOutputStream outTemp = new DataOutputStream(soc.getOutputStream());
									outTemp.writeUTF(val);
									outTemp.writeUTF(val2);
									outTemp.writeUTF(val3);
									//System.out.println("Sent to " + soc + " " + val + " " + val2);									
								}
								reply += "Node add operation completed........";
							}
							//send nodesMap to control client
							out.writeUTF(val);
						}
						out.writeUTF(reply);												
						break;
						
					case "delete":
						ret = dNode.delNode(Integer.parseInt(arg[1]));
						
						if(ret == -1)
						{
							System.out.println("Node delete operation failed!!!!!!!!!");
							reply += "Node delete operation failed!!!!!!!!!";							
						}
						
						else
						{
							//updated nodesMap has to be sent to other data nodes.
							val = "update_map,";
							val2 = "update_list,";
							val3 ="update_ipPortMap,";
							val4 ="delete_sockMap,";
							
							//convert hash map to string
							val += nodesMapToStr();
							val2 += nodesListToStr();
							val3 += ipPortMapToStr();
							val4 += Integer.parseInt(arg[1]);
													
							//send nodesMap in string to all other data nodes.
							synchronized(dNode.dnSockMap)
							{
								for(Integer r : dNode.dnSockMap.keySet())
								{
									Socket soc = dNode.dnSockMap.get(r);
									DataOutputStream outTemp = new DataOutputStream(soc.getOutputStream());
									outTemp.writeUTF(val);
									outTemp.writeUTF(val2);
									outTemp.writeUTF(val3);
									
									//System.out.println("--- " + r + " " + soc + " ----");
									//if(r != Integer.parseInt(arg[1]))
									outTemp.writeUTF(val4);
								}
								//send nodesMap to control client
								out.writeUTF(val);
								reply += "Node delete operation completed......";
							}
						}
						out.writeUTF(reply);						
						break;
						
					case "connect":						
						String[] nodes = arg[1].split("#");
						synchronized(dNode.dnSockMap)
						{
							for(String s : nodes)
							{
								String[] node = s.split("_");
								Socket client = new Socket(node[0], Integer.parseInt(node[1]));													
								
								dNode.dnSockMap.put(Integer.parseInt(node[2]), client);
							
								new DataNodeThread(client, dNode, false).start();	
								//System.out.format("Connection received from %s  %s\n", client.getRemoteSocketAddress().toString(), client.getLocalAddress().toString());
								//System.out.println(dNode.dnSockMap);
							}						
						}						
						break;
						
					case "update_map":
						strTonodesMap(arg[1]);
						break;
						
					case "update_list":
						strTonodesList(arg[1]);
						break;
						
					case "update_ipPortMap":
						strToipPortMap(arg[1]);
						break;
						
					case "delete_sockMap":
						synchronized(dNode.dnSockMap)
						{
							if(dNode.dnSockMap.containsKey(Integer.parseInt(arg[1])))
								dNode.dnSockMap.remove(Integer.parseInt(arg[1]));
						}
						break;
						
					case "replace_sockMap":
						synchronized(dNode.dnSockMap)
						{
							String[] values = arg[1].split("#");
							int oldV = Integer.parseInt(values[0]);
							int newV = Integer.parseInt(values[1]);							
							//System.out.println(dNode.dnSockMap);
							Socket temp = dNode.dnSockMap.get(oldV);
							if(temp != null)
							{
								dNode.dnSockMap.remove(oldV);
								dNode.dnSockMap.put(newV, temp);
							}
						}
						break;
						
					case "load_bal":
						String value = dNode.loadBalance(Integer.parseInt(arg[1]));
						if(!value.isEmpty())
						{
							val = "update_map,";
							val2 = "update_list,";
							val3 = "update_ipPortMap,";
							val4 = "replace_sockMap,";
							
							//convert hash map to string
							val += nodesMapToStr();
							val2 += nodesListToStr();
							val3 += ipPortMapToStr();
							val4 += value;
													
							//send nodesMap in string to all other data nodes.
							synchronized(dNode.dnSockMap)
							{
								for(Integer r : dNode.dnSockMap.keySet())
								{
									Socket soc = dNode.dnSockMap.get(r);
									DataOutputStream outTemp = new DataOutputStream(soc.getOutputStream());
									outTemp.writeUTF(val);
									outTemp.writeUTF(val2);
									outTemp.writeUTF(val3);
									
									//System.out.println("--- " + r + " " + soc + " ----");
									//System.out.println(r + " " + Integer.parseInt(arg[1]));
									//if(r != Integer.parseInt(arg[1]))
									outTemp.writeUTF(val4);
								}
							}
							//send nodesMap to control client
							out.writeUTF(val);
							
							//send updated ring id to control client							
							String result = "update_rid," + value;							
							out.writeUTF(result);
							
							reply += "Load balance operation completed.........";							
						}
						else
						{
							System.out.println("Load balance operation failed!!!!!!!!");
							reply += "Load balance operation failed!!!!!!!!";							
						}						
						out.writeUTF(reply);
						break;
						
					case "table_request":
						val = "update_map,";
						val2 = "update_list,";
						val3 = "update_ipPortMap,";
						val4 = "table_update_complete";
						
						val += nodesMapToStr();
						val2 += nodesListToStr();
						val3 += ipPortMapToStr();
						
						out.writeUTF(val);
						out.writeUTF(val2);
						out.writeUTF(val3);						
						out.writeUTF(val4);
						break;
						
					case "look_up":
						String replicas = dNode.lookupByRid(Integer.parseInt(arg[1]), Integer.parseInt(arg[2]));
												
						if(!replicas.isEmpty())
						{															
							out.writeUTF("Found," + dNode.ipMap.get(-1) + "," + replicas);							
						}
						else
						{
							out.writeUTF("Not_Found," + dNode.ipMap.get(-1));
						}
						break;
						
					case "control_client":
						dNode.ccSocket = socket;
						break;
						
					default:
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
