public class PageTableRecord {
   public String tablename;
    //public int pageno;
    public boolean dirtybit;
    public boolean isfull;
    public boolean incache;
    public int cacheindex;
    public PageTableRecord(String tablename,boolean incache,int cacheindex){
        this.tablename= tablename;
        //this.pageno= pageno;
        //this.dirtybit=dirtybit;
        //this.isfull=isfull;
        this.incache=incache;  
        this.cacheindex=cacheindex;
    }

}
