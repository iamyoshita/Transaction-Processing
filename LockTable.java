import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
//import java.lang.*;
public class LockTable {

    public class lockentry{
        String tid;
        String locktype;
        String userid;
        lockentry(String t,String l,String u){
            this.tid=t;
            this.locktype=l;
            this.userid=u;

        }
    }
    public class LTEntry{
        LinkedList<lockentry> grantedlist;
        LinkedList<lockentry> waitlist;

        LTEntry(){
            grantedlist=new LinkedList<lockentry>();
            waitlist=new LinkedList<lockentry>();
        }

    }

    //<itemname,entry> entry has 2 lists. granted and waitlist of node<tid,locktype>
    public HashMap<String,LTEntry> LT;//itemname,
    LockTable(){
        LT=new HashMap<String,LTEntry>();
    }

    boolean acquireLock(String item,String tid,String locktype,String userid){
        //for file lock item=tablename
        //for record lock, the item=tablename_userid
        if(!LT.containsKey(item)){//lock table doesn't have this item entry
            LTEntry row=new LTEntry();
            row.grantedlist.addLast(new lockentry(tid, locktype,userid));
            LT.put(item,row);
            return true;//granted lock as this is the first.
        }

        //check if this tid already has the lock for this entry.
        LTEntry row=LT.get(item);
        for(lockentry le:row.grantedlist){
            if(le.tid.equals(tid) && le.locktype.equals(locktype) && le.userid.equals(userid)){
                return true;
            }
        }

        if(!userid.equals("")){
            //file level lock
            boolean isputIntoWaitList=false;
    
            // printing the elements of LinkedHashMap
            for (lockentry le : row.grantedlist) {
                if(le.locktype.equals("T-MREAD")&& (locktype.equals("T-WL")||locktype.equals("P-WL"))){
                    isputIntoWaitList=true;
                    break;
                }else if(le.locktype.equals("T-WL") && (locktype.equals("T-MREAD"))){
                    isputIntoWaitList=true;
                    break;
                }
                else if(le.locktype.equals("P-WL") && (locktype.equals("T-MREAD"))){
                    isputIntoWaitList=true;
                    break;
                }
            }
            if(isputIntoWaitList){
                //waitlist.
                row.waitlist.addLast(new lockentry(tid, locktype,userid));
                return false;

            }else{
                row.grantedlist.addLast(new lockentry(tid, locktype,userid));
                return true;//granted lock

            }
        }
        else{
            //record level lock
            boolean isputIntoWaitList=false;
        
            // printing the elements of LinkedHashMap
            for (lockentry le : row.grantedlist) {
                if(le.locktype.equals("T-WL")||le.locktype.equals("P-WL")){
                    isputIntoWaitList=true;
                    break;
                }else if(le.locktype.equals("T-RL") && (locktype.equals("T-WL")||locktype.equals("P-WL"))){
                    isputIntoWaitList=true;
                    break;
                }
            }
            if(isputIntoWaitList){
                //waitlist.
                row.waitlist.addLast(new lockentry(tid, locktype,""));
                return false;

            }else{
                row.grantedlist.addLast(new lockentry(tid, locktype,""));
                return true;//granted lock

            }

        }
    }

    String releaseLock(String item,String tid,String userid){
        //returns null if no other tid has lock on this item
        //returns 

        LTEntry row=LT.get(item);
        if(row==null){
            return "";
        }
        lockentry le=row.grantedlist.getFirst();
        if(le.tid.equals(tid) && le.userid.equals(userid)){
            row.grantedlist.removeFirst();

        
            //--if first item in granted list is removed
            //if this was the only one in granted and waited. then delete row. from LT, return NONE
            if(row.grantedlist.size()==0 && row.waitlist.size()==0){
                LT.remove(item);
                return "";
            }

            //if the granted list had another item, then return its locktype HEAD CHANGE.
            if(row.grantedlist.size()!=0){
                return "changed "+row.grantedlist.getFirst().tid+" "+row.grantedlist.getFirst().locktype+" "+row.grantedlist.getFirst().userid;//changed newtid newlocktype

            }

            if(row.grantedlist.size()==0){

            //if granted list empty but waitlist is not. then pop from waitlist and insert to grantlist
            //return status GRANTED and the new tid to scheduler so that it can restart the tid
            //
            lockentry newboss=row.waitlist.getFirst();
            row.grantedlist.add(newboss);
            row.waitlist.removeFirst();
            return "granted "+row.grantedlist.getFirst().tid+" "+row.grantedlist.getFirst().locktype+" "+row.grantedlist.getFirst().userid;

            }
        }
        //--if middle item of granted list is removed then return LOCKED signal. in the case of mread
        else{
            int ind=0;
            for(lockentry l:row.grantedlist){
                if(l.tid.equals(tid) && l.userid.equals(userid)){
                    break;
                }
                ind++;
            }
            if(ind<row.grantedlist.size())
                row.grantedlist.remove(ind);
            return "locked";//had the same tid. no change. file still locked

        }
        return "";
    }

    //returns list of aborted transactions
    ArrayList<String> detectDeadlockLT(){
        String addpythoncode="";
        ArrayList<String> abortedt=new ArrayList<String>();
        for(String key:LT.keySet()){
            LTEntry row=LT.get(key);
            for(lockentry g: row.grantedlist){
                if(!(Scheduler.abortedTransactions.indexOf(g.tid)>=0)){//not in aborted transactions
                    for(lockentry w: row.waitlist){
                        if(!(Scheduler.abortedTransactions.indexOf(w.tid)>=0)){//not in aborted transactions
                            String temp=addpythoncode;
                            addpythoncode+="print(d.wait_for("+w.tid+", "+g.tid+"))\n";
                            if(check_python_deadlock(addpythoncode)){
                                Scheduler.abortedTransactions.add(w.tid);//deadlock exists, so abort the waiting transaction
                                System.out.println("ABORTED TRANSACTION/PROCESS"+w.tid);
                                Logging.logOperation("deadlock detected, aborting"+w.tid+" \n");
                                addpythoncode=temp;
                                abortedt.add(w.tid);
                                addpythoncode+="d.transaction_ended("+w.tid+")\n";
                            }
                            else{
                                //because the py file must have only one print statement.
                                addpythoncode=temp;
                                addpythoncode+="d.wait_for("+w.tid+", "+g.tid+")\n";
                            }

                        }
                    }
                }
            }

        }

 
    return abortedt;
    }

    static boolean check_python_deadlock(String addpythoncode){
        //addpythoncode+="print(d.find_deadlock_cycle())";
        try{

            var source = new File("DeadlockDetector.py");
            var dest = new File("wfg.py");
    
            try (var fis = new FileInputStream(source);
                 var fos = new FileOutputStream(dest)) {
    
                byte[] buffer = new byte[1024];
                int length;
    
                while ((length = fis.read(buffer)) > 0) {
    
                    fos.write(buffer, 0, length);
                }
            }

            FileWriter fw=new FileWriter("DeadlockDetector.py",true);
            fw.write(addpythoncode+"\n");
            fw.close();
            ProcessBuilder processBuilder = new ProcessBuilder("C:/Users/Yoshita/AppData/Local/Programs/Python/Python39/python.exe", "C:/Users/Yoshita/Desktop/CS2550-DBMS-project-1/DeadlockDetector.py");
            processBuilder.redirectErrorStream(true);
        
            Process process = processBuilder.start();
            InputStream inputstream = process.getInputStream();
            String text = new BufferedReader(
            new InputStreamReader(inputstream))
                .lines()
                .collect(Collectors.joining("\n"));
                System.out.println("\n***THE CYCLE DATA IS****"+text);
                    inputstream.close();
            



            //rename file
            File ff=new File("DeadlockDetector.py");
            ff.delete();
            File file = new File("wfg.py");
  
            File rename = new File("DeadlockDetector.py");

            boolean flag = file.renameTo(rename);

            if(!text.equals("None")){//cycle exists
                return true;
            }
            return false;
        }catch(Exception e){
            System.out.print("cannot open python file"+e);
            return false;

        }
    }
    // public static void main(String args[]){
    //     String addpythoncode="print(d.wait_for(1,2))\nprint(d.wait_for(2,1))\n";
    //     check_python_deadlock(addpythoncode);
        
    // }
}
