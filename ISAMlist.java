import java.util.*;

public class ISAMlist{
    public ArrayList<ISAMnode> accesslist;
    public ISAMlist(){
        accesslist=new ArrayList<ISAMnode>(); 
    }
    public void addlast(int id,int block,int x){ //fromid,blockno,toid
        ISAMnode i=new ISAMnode(id,block,x);
        accesslist.add(i);
    }
    public void removeRange(int index){
        //accesslist.removeRange(index,accesslist.size());
    }
    public int read(int userid){
        Comparator<ISAMnode> c = new Comparator<ISAMnode>() {
            public int compare(ISAMnode u1, ISAMnode u2)
            {
                return u1.user_id-(u2.user_id);
            }
        };
        int index= Collections.binarySearch(accesslist,new ISAMnode(userid,-1,-1),c); 
        if (index < 0)
        {
            index= ~index - 1;
        }
        if (index >= 0)
        {
            return index;
        }
        return 0;//no element smaller than userid.
    }
    public int get(int userid)
    { 
        Comparator<ISAMnode> c = new Comparator<ISAMnode>() {
            public int compare(ISAMnode u1, ISAMnode u2)
            {
                return u1.user_id-(u2.user_id);
            }
        };
        int index= Collections.binarySearch(accesslist,new ISAMnode(userid,-1,-1),c); 
        if (index < 0)
        {
            int a1= ~index - 1;
            int a2=~index;
            System.out.println("\na1 and a2:"+a1+" "+a2);
                   //     System.out.println("\nto userid and userid is:"+accesslist.get(a1).touser_id+" "+userid);
            if((a1>=0)&&accesslist.get(a1).touser_id>userid){
                index = ~index - 1;//closest smaller value
            }
            else{
                index = ~index;//closest value
            }
        }
        if (index >= 0)
        {
            return index;
        }
        return -1;//no element smaller than userid.
    } 


    
}
