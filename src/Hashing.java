/**
* Naive Hash Implementation  
* @author  Anoop S Somashekar
* @version 1.0
* @since   2016-11-20
*/
public class Hashing
{
	/**
	 * computes of the given string using java default hash code
	 * @param data
	 * @return
	 */
	public int computeHash(String data)
	{
		System.out.println("Default Hashing");
		return Math.abs(data.hashCode());
	}
}