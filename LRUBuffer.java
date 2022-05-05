import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
public class LRUBuffer{

	LinkedHashMap<Integer,CacheNode> cache;
	public int cachesize;
	public class clsmNode{
		String table;
		int level;
		int key1; 
		int key2;
		public clsmNode(String a,int b,int c,int d){
			this.table=a;
			this.level=b;
			this.key1=c;
			this.key2=d;
		}
	}
	LinkedHashMap<clsmNode,TreeMap<Integer,Record>> lsmcache;

	public LRUBuffer(int cachesize)
	{
		this.cache = new LinkedHashMap<Integer,CacheNode>();
		//System.out.println("cachesize is "+cachesize);
		this.cachesize = cachesize;
		this.lsmcache=new LinkedHashMap<clsmNode,TreeMap<Integer,Record>>();
	}
	public CacheNode get(int key)
	{
		if (!cache.containsKey(key))
			return new CacheNode(" cache get error",-1," ");
		CacheNode val=cache.remove(key);
		cache.put(key,val);
		//System.out.println("\ncache get has data:"+val.data);
		return val;
	}

	public CacheNode put(int key,CacheNode newval)
	{			
		CacheNode temp=new CacheNode(newval);
        CacheNode replacedpage;
		if(cache.containsKey(key)){ //updating an existing block in a cache
			cache.remove(key);
			cache.put(key,temp);
			return null;
		}
	if (cache.size() == cachesize) { // if cache is full
			//System.out.println("\ncache.size() is "+cache.size());
			int replacedpageindex = cache.keySet().iterator().next();
			replacedpage=cache.remove(replacedpageindex);
            cache.put(key,temp);
            return replacedpage;

		}
		cache.put(key,temp);
		//System.out.println("cache key is "+key);
		//System.out.println("\ncache val is "+temp.data);
        return null;
	}

	public Record getlsm(String table,int userid){
		//
		Record ans=null;
		for (Map.Entry<clsmNode,TreeMap<Integer,Record>> mapElement :  lsmcache.entrySet()) {

			clsmNode key = mapElement.getKey();
			if(key.table.equals(table)){
				TreeMap<Integer,Record> block = mapElement.getValue();
				if(block.containsKey(userid)){
					lsmcache.remove(key);
					lsmcache.put(key,block);
					ans=block.get(userid);
					break;
				}
			}
   		}
		return ans;
	}

	public void addlsm(String table, int level,TreeMap<Integer,Record> block){
		//add this block to the cache.
		//if cache is already full, the replace the LRU block
		//key=tablename+level+
		String key1=block.firstKey()+"";
		String key2=block.lastKey()+"";
		if(lsmcache.size()==cachesize){
			clsmNode replacednode = lsmcache.keySet().iterator().next();
			Logging.logSwapLSM(false, replacednode.level, replacednode.table, replacednode.key1, replacednode.table, replacednode.key2);
			lsmcache.remove(replacednode);
		}
		clsmNode key=new clsmNode(table,level,Integer.parseInt(key1),Integer.parseInt(key2));
		lsmcache.put(key,block);
		Logging.logSwapLSM(true, level,table,Integer.parseInt(key1),table, Integer.parseInt(key2));

	}

	public void putlsm(String table, Record r){
		//replace the score of r.userid in the block
		for (Map.Entry<clsmNode,TreeMap<Integer,Record>> mapElement :  lsmcache.entrySet()) {

			clsmNode key = mapElement.getKey();
			if(key.table.equals(table)){
				TreeMap<Integer,Record> block = mapElement.getValue();
				if(block.containsKey(r.user_id)){
					block.put(r.user_id,r);
					lsmcache.remove(key);
					lsmcache.put(key,block);
					//System.out.println("\nupdated in cache\n");
				}
			}
   		}


	}

}
