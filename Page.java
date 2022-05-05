public class Page{
    public int diskblocksize;
    public byte[] bytes;
    public Page(int siz){
        this.diskblocksize=siz;
        bytes = "".getBytes();

  // byte[] to string
  //String s = new String(bytes);
        //this.bytes
    }

    
}