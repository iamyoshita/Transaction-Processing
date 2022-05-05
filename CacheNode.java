public class CacheNode{
    public String table;
    public int pageno;
    public String data;
    public CacheNode(String table,int pageno,String data){
        this.table=table;
        this.pageno=pageno;
        this.data=data+"";
    }
    public CacheNode(CacheNode cn){
        this.table=cn.table;
        this.pageno=cn.pageno;
        this.data=cn.data+"";
    }
}