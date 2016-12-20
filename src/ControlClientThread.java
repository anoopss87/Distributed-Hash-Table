import java.io.*;
import java.net.*;

/**
* Control Client thread class.  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class ControlClientThread extends Thread
{
	private Socket socket = null;
	private ControlClient cClient = null;	
	
	/**
	 * Constructor
	 * @param socket
	 * @param dn
	 */
    public ControlClientThread(Socket socket, ControlClient cc)
    {
        super("ControlClientThread");
        this.socket = socket;
        this.cClient = cc;               
    }
    
    /**
     * Converts string to hash map
     * @param str
     */
    synchronized public void strTonodesMap(String str)
    {    	
    	String[] entry = str.split("#");
    	
    	synchronized (cClient.nodesMap)
    	{		
    		cClient.nodesMap.clear();
    		for(String kv : entry)
    		{
    			String[] t = kv.split("_");
    			int r = Integer.parseInt(t[0]);
    			int p = Integer.parseInt(t[1]);
    			int s = Integer.parseInt(t[2]);
    			DHTRingNode rn = new DHTRingNode(r, p, s);
    			cClient.nodesMap.put(r, rn);
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
			while(true)
	    	{
				String data = in.readUTF();    			
					                
				String[] arg = data.split(",");    
				
				switch(arg[0])
				{
					case "update_rid":
						String[] values = arg[1].split("#");
						int oldV = Integer.parseInt(values[0]);
						int newV = Integer.parseInt(values[1]);
												
						synchronized(cClient.ridList)
						{
							int index = cClient.getRingIdIndex(oldV);
							cClient.ridList.remove(index);
							cClient.ridList.add(newV);
						}
						
						synchronized(cClient.ipMap)
						{
							String val = cClient.ipMap.get(oldV);
							cClient.ipMap.remove(oldV);
							cClient.ipMap.put(newV, val);
						}
						
						synchronized(cClient.portMap)
						{
							String val = cClient.portMap.get(oldV);
							cClient.portMap.remove(oldV);
							cClient.portMap.put(newV, val);
						}
						
						synchronized(cClient.sockMap)
						{
							Socket temp = cClient.sockMap.get(oldV);
							cClient.sockMap.remove(oldV);
							cClient.sockMap.put(newV, temp);
						}
						break;
						
					case "reply":
						System.out.println(arg[1]);
						double estTime = (System.currentTimeMillis() - cClient.startTime);
						System.out.format("The total time for control client opeation is %f milli seconds\n", estTime);
						break;
						
					case "update_map":
						strTonodesMap(arg[1]);
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
