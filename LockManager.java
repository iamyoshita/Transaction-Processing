import java.util.*;
public class LockManager {

    public class treenode{
        String type;
        String item;
        String status;
        String locktype;
        public ArrayList<treenode> subtree;

        treenode(String t,String u, String s){
            this.type=t;
            this.item=u;
            this.status=s;
            this.subtree=new ArrayList<treenode>();
        }
    }

    treenode gtree;
    LockTable lt;
    LockManager(){
        lt=new LockTable();
        gtree=new treenode("DB","-1","available");
        gtree.locktype="none";

    }

    int getLevel(String filename){
        for(treenode t:gtree.subtree){
            if(t.type.equals("file") && t.item.equals(filename)){
                //System.out.println("returning subtree index"+gtree.subtree.indexOf(t)+"\n");
                return gtree.subtree.indexOf(t);
            }
        }
        System.out.println("returning subtree index -1 \n");

        return -1;
    }

    boolean addlock(String table,String userid,String tid,String locktype){
        int treeindex=getLevel(table);
        if(treeindex==-1){
            if(lt.acquireLock(table, tid, locktype,userid)){

                if(locktype.equals("T-MREAD")){
                    //this file is locked for the first time
                    //if(lt.acquireLock(table, tid, locktype,userid)){
                        //got file lock
                        treenode t=new treenode("file",table,"granted");
                        t.locktype=locktype;
                        gtree.subtree.add(t);
                        return true;
                    }
                
            //}else{//record level lock
                if(lt.acquireLock(table+"_"+userid, tid, locktype, "")){
                    treenode t=new treenode("record",userid,"granted");
                    t.locktype=locktype;
                    treenode t1=new treenode("file",table,"granted");
                    t1.locktype=locktype;//record is locked
                    t1.subtree.add(t);
                    gtree.subtree.add(t1);
                    return true;
                }
               // else{                //}

            }
            return false;
        }

        //file already has some lock;
            if(lt.acquireLock(table, tid, locktype,userid)){
                gtree.subtree.get(treeindex).locktype=locktype;
                gtree.subtree.get(treeindex).status="granted";
                if(locktype.equals("T-MREAD")){
                return true;
                }
        //}else{//record level lock
            //file either has a t-mread lock or its record had a lock.
            //if file is reading with t-mread lock then we cannot WL on a record.
            //we can only RL on the record.
            // if(gtree.subtree.get(treeindex).locktype.equals("T-MREAD") && (locktype.equals("T-WL")||locktype.equals("P-WL"))){
            //     lt.acquireLock(table, tid, locktype, "");
            //     return false;
            // }
            if(lt.acquireLock(table+"_"+userid, tid, locktype, "")){
                //if we already had a lock on the same record userid
                for(treenode t:gtree.subtree.get(treeindex).subtree){
                    if(t.item.equals(userid)){
                        t.locktype=locktype;
                        t.status="granted";
                        return true;
                    }
                }
                //acquiring the record lock for the first time for this userid
                treenode t=new treenode("record", userid,"granted");
                t.locktype=locktype;
                gtree.subtree.get(treeindex).subtree.add(t);
                return true;

            }
            // }else{
            //     return false;
            // }
        }
        return false;

    }

    String removeLock(String table,String userid,String tid,String locktype){
        int treeindex=getLevel(table);
        if(treeindex==-1){
            return "failed";
        }

        if(!gtree.subtree.get(treeindex).status.equals("granted")){
            return "";//table andits record are not locked. so no need to release
        }

        if(locktype.equals("T-MREAD")){
            //just release the file level lock.its record can have real locks
            String response = lt.releaseLock(table, tid, userid);

            if(response.equals("")){
                //the file has been unlocked/released
                gtree.subtree.get(treeindex).status="not granted";
                gtree.subtree.get(treeindex).locktype="";
                return "none";

            }

            if(response.equals("locked")){
                // the file is still mread locked by another transaction
                return "none";
            }

            if(response.split(" ")[0].equals("changed")){
                // table still locked by another transaction
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=response.split(" ")[2];
                return "none";

            }

            if(response.split(" ")[0].equals("granted") && response.split(" ")[2].equals("T-MREAD")){
                //new tid from waitlist got the lock
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=response.split(" ")[2];
                return response.split(" ")[1];//return the tid of the new txn that got the lock
            }

            //todo: what if the mread is released and some tid wanted WL on record level. Hence store all pending tids in a list and call them??

            if(lt.acquireLock(table+"_"+response.split(" ")[3], response.split(" ")[1], response.split(" ")[2], "")){

                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=response.split(" ")[2];
                return updateAtIndex(treeindex, response.split(" ")[3], response.split(" ")[2], response.split(" ")[1], "granted");


            }

            System.out.print("\n something wrong in removeLock did it reach here??\n");
            return "none";

        }


        //remove a lock of type T-read,T-WL P-WL
        System.out.println("user id is "+userid);
        String recres=lt.releaseLock(table+"_"+userid, tid, "");
        String fileres=lt.releaseLock(table, tid, userid);

        if(recres.equals("")){
            updateAtIndex(treeindex, userid,"", "", "not granted");
            //record is unlocked. now unlock file if no other record is locked in that file

            if(fileres.equals("")){
                gtree.subtree.get(treeindex).status="not granted";
                gtree.subtree.get(treeindex).locktype="";
                return "none";
            }

            if(fileres.equals("locked")){
                return "none";
            }

            if(fileres.split(" ")[0].equals("changed")){
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return "none";

            }

            if(fileres.split(" ")[0].equals("granted") && fileres.split(" ")[2].equals("T-MREAD")){
                //new tid from waitlist got the lock
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return fileres.split(" ")[1];//return the tid of the new txn that got the lock
            }

            //todo: what if the mread is released and some tid wanted WL on record level. Hence store all pending tids in a list and call them??
System.out.println(fileres);
            if(lt.acquireLock(table+"_"+fileres.split(" ")[3], fileres.split(" ")[1], fileres.split(" ")[2], "")){
                updateAtIndex(treeindex, fileres.split(" ")[3], fileres.split(" ")[2], fileres.split(" ")[1], "granted");
                
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return fileres.split(" ")[1];
            }else{
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return "none";

            }

            //System.out.print("\n something wrong in removeLock did it reach here??\n");
           // return "none"

        }


        //update the record node in gtree. but record is locked.
        if(recres.equals("locked")){
            if(fileres.equals("")){
                return "-1";
            }

            if(fileres.equals("locked")){
                return "none";
            }

            if(fileres.split(" ")[0].equals("changed")){
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return "none";

            }

            if(fileres.split(" ")[0].equals("granted") && fileres.split(" ")[2].equals("T-MREAD")){
                //new tid from waitlist got the lock
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return fileres.split(" ")[1];//return the tid of the new txn that got the lock
            }

            //todo: what if the mread is released and some tid wanted WL on record level. Hence store all pending tids in a list and call them??

            if(lt.acquireLock(table+"_"+fileres.split(" ")[3], fileres.split(" ")[1], fileres.split(" ")[2], "")){
                updateAtIndex(treeindex, fileres.split(" ")[3], fileres.split(" ")[2], fileres.split(" ")[1], "granted");
                
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return fileres.split(" ")[1];
            }else{
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return "none";

            }




        }

        if(recres.split(" ")[0].equals("changed")){
            updateAtIndex(treeindex, recres.split(" ")[3], recres.split(" ")[2], recres.split(" ")[1], "granted");


            if(fileres.equals("")){
                return "-1";
            }

            if(fileres.equals("locked")){
                return "none";
            }

            if(fileres.split(" ")[0].equals("changed")){
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return "none";

            }

            if(fileres.split(" ")[0].equals("granted") && fileres.split(" ")[2].equals("T-MREAD")){
                //new tid from waitlist got the lock
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return fileres.split(" ")[1];//return the tid of the new txn that got the lock
            }

            //todo: what if the mread is released and some tid wanted WL on record level. Hence store all pending tids in a list and call them??

            if(lt.acquireLock(table+"_"+fileres.split(" ")[3], fileres.split(" ")[1], fileres.split(" ")[2], "")){
                updateAtIndex(treeindex, fileres.split(" ")[3], fileres.split(" ")[2], fileres.split(" ")[1], "granted");
                
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return fileres.split(" ")[1];
            }else{
                gtree.subtree.get(treeindex).status="granted";
                gtree.subtree.get(treeindex).locktype=fileres.split(" ")[2];
                return "none";

            }


        }
        String id="";
        if( recres.split(" ").length==4){
            id=recres.split(" ")[3];
        }

        return updateAtIndex(treeindex, id, recres.split(" ")[2], recres.split(" ")[1], "granted");

    }

    String updateAtIndex(int ind,String userid, String nlocktype,String ntid,String nlstatus){
        
        for(treenode k:gtree.subtree.get(ind).subtree){
            if(k.item.equals(userid)){
                k.locktype=nlocktype;
                k.status=nlstatus;
                return ntid;
            }
        }
        treenode t=new treenode("record", userid,nlstatus);
        gtree.subtree.get(ind).subtree.add(t);
        return ntid;

    }

    ArrayList<String> deadlockDeadlock(){
        return lt.detectDeadlockLT();
    }





    
}
