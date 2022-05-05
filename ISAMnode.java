public class ISAMnode{
    public int user_id;
    public int touser_id;
    public int blockno;
    public ISAMnode(int a,int b,int c){
        this.user_id=a;
        this.blockno=b;
        this.touser_id=c;
    }
}