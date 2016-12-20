/**
* Ring Node Class  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/

class DHTRingNode
{
	//successor of a node in the ring
	private int succ;
	//predecessor of a node in the ring
	private int pred;	
	//computed hash values of the node(ip address)
	private int rId;			
	
	/**
	 * Constructor
	 * @param id
	 */
	DHTRingNode(int id)
	{
		rId = id;			
		succ = rId;
		pred = rId;						
	}
	
	/**
	 * Constructor with multiple parameters
	 * @param id
	 * @param p
	 * @param s
	 */
	DHTRingNode(int id, int p, int s)
	{
		rId = id;
		pred = p;
		succ = s;								
	}
	
	/**
	 * set predecessor
	 * @param p
	 */
	void setPred(int p)
	{
		pred = p;
	}
	
	/**
	 * set successor
	 * @param s
	 */
	void setSucc(int s)
	{
		succ= s;
	}
	
	/**
	 * get predecessor
	 * @return
	 */
	int getPred()
	{
		return pred;
	}
	
	/**
	 * get successor
	 * @return
	 */
	int getSucc()
	{
		return succ;
	}		
	
	/**
	 * get ring id
	 * @return
	 */
	int getrId()
	{
		return rId;
	}
	
	/**
	 * set ring id
	 * @param r
	 */
	void setrId(int r)
	{
		rId = r;
	}
	
	/**
	 * does this ring node holds the key
	 * @param id
	 * @return
	 */
	boolean isHolder(int id)
	{			
		if(rId == id)
		{
			return true;
		}
		else if(pred < rId)
		{
			if(id > pred && id < rId)
			{
				return true;
			}
		}
		else if(id > pred || id < rId)
		{
			return true;
		}
		return false;
	}
}	
