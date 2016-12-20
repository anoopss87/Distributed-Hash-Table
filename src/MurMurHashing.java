/**
* Murmur Hash Implementation  
* Ported from open source implementation
* @version 1.0
* @since   2016-11-20
*/
public class MurMurHashing extends Hashing
{
	/**
	 * computes hash value for the given data using murmur hash
	 * @param data
	 * @param offset
	 * @param len
	 * @param seed
	 * @return
	 */
	public int computHash(String data, int offset, int len, int seed)
	{
		return Math.abs(murmurhash3_x86_32(data.getBytes(), offset, len, seed));
	}
	
	/**
	 * computes hash value for the given data using murmur hash
	 */
	public int computeHash(String data)
	{		
		return Math.abs(murmurhash3_x86_32(data.getBytes(), 0, data.length(), 1));
	}
	
	/**
	 * computes hash value for the given data using murmur hash
	 * @param data
	 * @param offset
	 * @param len
	 * @param seed
	 * @return
	 */
	public int murmurhash3_x86_32(byte[] data, int offset, int len, int seed)
	{
	    final int c1 = 0xcc9e2d51;
	    final int c2 = 0x1b873593;

	    int h1 = seed;
	    int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

	    for (int i=offset; i<roundedEnd; i+=4)
	    {
	      // little endian load order
	    	int k1 = (data[i] & 0xff) | ((data[i+1] & 0xff) << 8) | ((data[i+2] & 0xff) << 16) | (data[i+3] << 24);
	    	k1 *= c1;
	    	k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
	    	k1 *= c2;

	    	h1 ^= k1;
	    	h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
	    	h1 = h1*5+0xe6546b64;
	    }

	    // tail
	    int k1 = 0;

	    switch(len & 0x03)
	    {
	    	case 3:
	    		k1 = (data[roundedEnd + 2] & 0xff) << 16;
	    		// fallthrough
	    	case 2:
	    		k1 |= (data[roundedEnd + 1] & 0xff) << 8;
	    		// fallthrough
	    	case 1:
	    		k1 |= (data[roundedEnd] & 0xff);
	    		k1 *= c1;
	    		k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
	    		k1 *= c2;
	    		h1 ^= k1;
	    }

	    // finalization
	    h1 ^= len;

	    // fmix(h1);
	    h1 ^= h1 >>> 16;
	    h1 *= 0x85ebca6b;
	    h1 ^= h1 >>> 13;
	    h1 *= 0xc2b2ae35;
	    h1 ^= h1 >>> 16;

	    return h1;
	}
}