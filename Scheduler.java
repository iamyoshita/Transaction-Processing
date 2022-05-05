import java.util.*;
public class Scheduler {
    public class Lock{
        String locktype;
        String table;
        String userid;
        
    }
    SeqFileStrategy sfs=null;
    LSMStrategy lsm=null;

    public class TidData{
        ArrayList<Lock> acquiredLocks;
        String waiton;//which Tid it is waiting for
        ArrayList<String> delayedOp;//instr it is waiting on
        long starttime;//System.currentTimeMillis()
        String execmode;// T or P

        TidData(String e){
            this.acquiredLocks=new ArrayList<Lock>();
            this.delayedOp=new ArrayList<String>();
            this.starttime=System.currentTimeMillis();
            this.execmode=e;
            this.waiton="";

        }
    }
    public static ArrayList<String> abortedTransactions;
    public static HashMap<String,TidData> activeTids;
    public HashMap<String,Integer> fnameTotid;
    public static int next_tid=1;
    public int totalcommands=0;
    public long readtime=0;
    public long writetime=0;
    public int totalcommits=0;
    public int totalreads=0;
    public int totalwrites=0;

    public static RecoveryManager rm=new RecoveryManager();
    public static LockManager lm=new LockManager();
    Scheduler(SeqFileStrategy sfs){
        abortedTransactions=new ArrayList<String>();
        fnameTotid = new HashMap<String,Integer>();
        activeTids=new HashMap<String,TidData>();
        this.sfs = sfs;

    }
    Scheduler(LSMStrategy lsms){
        abortedTransactions=new ArrayList<String>();
        fnameTotid = new HashMap<String,Integer>();
        activeTids=new HashMap<String,TidData>();
        this.lsm = lsms;

    }

    public void execute(String instr,String fname){
        totalcommands+=1;
        System.out.println(instr);
        int tid=-1;
        if(!(instr.split(" ")[0].equals("B"))){
            if(fnameTotid.get(fname)==null){
                System.out.println("wrong input. no tid for it"+fname);
                return;
            }
            tid=fnameTotid.get(fname);
        } 
        
        for(String i: abortedTransactions){
            if(i.equals(tid+"")){
                //stats.abortscount++
                System.out.println("instr from aborted txn."+i+" returning");

                return;
            }
        }

        if(acquireLock(instr,tid+"")){
            if (instr.split(" ")[0].equals("R")){
                //stats.total_read_count += 1;
                run_read(instr, tid+"");
                if(activeTids.get(tid+"").execmode.equals("P"))
                    release_process_locks(tid+"");
            }
            else if (instr.split(" ")[0].equals("I")){
                //stats.total_write_count += 1;
                run_write(instr, tid+"");
                if(activeTids.get(tid+"").execmode.equals("P"))
                    release_process_locks(tid+"");
            } else if (instr.split(" ")[0].equals("U")){
                //stats.total_write_count += 1;
                run_update(instr, tid+"");
                if(activeTids.get(tid+"").execmode.equals("P"))
                    release_process_locks(tid+"");
            
            }
            else if(instr.split(" ")[0].equals("M")){
                //stats.total_mread_count += 1;
                run_mread(instr, tid+"");
                if(activeTids.get(tid+"").execmode.equals("P"))
                    release_process_locks(tid+"");
            }
            else if (instr.split(" ")[0].equals("C")) {
                //fnameTotid.remove(fname); 
                run_commit(instr, tid+""); 
            }
            else if (instr.split(" ")[0].equals("A")) {
                //fnameTotid.remove(fname);
                run_abort(instr, tid+""); 
            }else if(instr.split(" ")[0].equals("B")){
                System.out.print("ENTERED B");
                tid=next_tid;
                fnameTotid.put(fname, tid);
                next_tid+=1;
                run_begin(instr,tid+"");
            }
        }

        if(totalcommands%10==0){
            detectdeadlock(instr);
        }

    }

    void detectdeadlock(String line){
        ArrayList<String> ab=lm.deadlockDeadlock();
        
        for (String x: ab){
            System.out.println("deadlock due to "+x+", so aborting it");
            abortedTransactions.add(x);
            abortT(x);//send tid;

        }


    }

    void abortT(String tid){

        rm.abort(tid);
        Logging.logAbort(tid);
        if(!activeTids.get(tid).waiton.equals("")){
            //release the waiting lock
            TidData td=activeTids.get(tid);
            String[] waitops=td.waiton.split("[ ,()]");
            String table=waitops[1];
            //stats.updateabortcount

            if(waitops[0].equals("R")){
                if(td.execmode.equals("T"))
                    lm.removeLock(table, waitops[2], tid, td.execmode+"-RL");

            }else if(waitops[0].equals("I")){
                lm.removeLock(table, waitops[2], tid, td.execmode+"-WL");

            }else if(waitops[0].equals("M")){
                if(td.execmode.equals("T"))
                lm.removeLock(table, waitops[2], tid, td.execmode+"-MREAD");


            }else if(waitops[0].equals("U")){
                lm.removeLock(table, waitops[2], tid, td.execmode+"-WL");

            }
        }
        releaseLocks(tid,true);

    }

    void releaseLocks(String tid,boolean isaborted){
        TidData td=activeTids.get(tid);
        if(td==null){
            return;
        }
        System.out.println("RELEASING LOCKS FOR TXN:"+tid);
        if(td.execmode.equals("T")){
            for(Lock l:td.acquiredLocks){
                String res="";
                System.out.println("l.locktype in releaselocks is"+l.locktype);

                if(l.locktype.equals("read")){
                    res=lm.removeLock(l.table, l.userid, tid, "T-RL");
                    readtime+=System.currentTimeMillis()-td.starttime;
                    totalreads++;

                }else if(l.locktype.equals("write")){
                    res=lm.removeLock(l.table, l.userid, tid, "T-WL");

                    writetime+=System.currentTimeMillis()-td.starttime;
                    totalwrites++;

                }else if(l.locktype.equals("mread")){
                    res=lm.removeLock(l.table, l.userid, tid, "T-MREAD");
                    readtime+=System.currentTimeMillis()-td.starttime;
                    totalreads++;


                }else if(l.locktype.equals("update")){
                    res=lm.removeLock(l.table, l.userid, tid, "T-WL");
                    writetime+=System.currentTimeMillis()-td.starttime;
                    totalwrites++;

                }
                canStartNewTxn(res);

                res="";
            }
            td.acquiredLocks.clear();
            activeTids.put(tid, td);
        }
        activeTids.remove(tid);
        System.out.println("remove tid"+tid+"from activer txns");
    }

    void canStartNewTxn(String res){
        if(res.equals("none")){
            return;
        }
        for(String ab: abortedTransactions){
            if(ab.equals(res))
            return;
        }
        System.out.println("in canStartNewTxn, res is"+res);
        TidData td=activeTids.get(res);
        String ww=td.waiton;
        String[] waitops=td.waiton.split("[ ,()]");
        String table=waitops[1];
         // Add the lock to the acquired locks list
        td.waiton="";

        if(waitops[0].equals("R")){
            Lock l=new Lock();
            l.locktype="read";
            l.table=table;
            l.userid=waitops[2];
            if(!lockInAcquired(res,l)){
                td.acquiredLocks.add(l);
            }

        }else if(waitops[0].equals("I")){
            Lock l=new Lock();
            l.locktype="write";
            l.table=table;
            l.userid=waitops[2];
            if(!lockInAcquired(res,l)){
                td.acquiredLocks.add(l);
            }

        }else if(waitops[0].equals("M")){
            Lock l=new Lock();
            l.locktype="mread";
            l.table=table;
            l.userid=waitops[2];
            if(!lockInAcquired(res,l)){
                td.acquiredLocks.add(l);
            }


        }else if(waitops[0].equals("U")){
            Lock l=new Lock();
            l.locktype="write";
            l.table=table;
            l.userid=waitops[2];
            if(!lockInAcquired(res,l)){
                td.acquiredLocks.add(l);
            }

        }
        activeTids.put(res, td);
        run(ww,res);
        td=activeTids.get(res);
        ArrayList<String> delayedOperations=new ArrayList<String>();
        for(String ig:td.delayedOp){
            delayedOperations.add(ig+"");
        }
        ArrayList<Integer> remlist=new ArrayList<Integer>();
        int idx=0;
            for(String dt:delayedOperations){
                System.out.println("delayed ops"+dt+" "+idx);
                if(acquireLock(dt, res)){
                    run(dt, res); 
                    //delayedOperations.remove(idx); 
                    td.delayedOp.remove(0);
                  //  remlist.add(idx);
                } else {
                    // Updated waiting op to this delayed op
                    td.waiton = delayedOperations.get(0); 
                    //delayedOperations.remove(idx);
                    //remlist.add(idx);
                    //td.delayedOp = delayedOperations; 
                    td.delayedOp.remove(0);

                    activeTids.put(res,td); 
                    break; 
                }
                idx++;
            }
            // for(int id:remlist){
            //     System.out.println(id);
            // td.delayedOp.remove(id);
            // }
            //td.delayedOp = delayedOperations; 
                    //activeTids.put(res,td); 
    }

    boolean lockInAcquired(String tid,Lock l){
        for(Lock x: activeTids.get(tid).acquiredLocks){
            if(x.locktype.equals(l.locktype) &&x.table.equals(l.table)&&x.userid.equals(l.userid)){
                return true;
            }
        }
        return false;
    }

    void run(String instr,String tid){


        if (instr.split("[ ,()]")[0].equals("R")){
            //stats.total_read_count += 1;
            run_read(instr, tid);
			if(activeTids.get(tid).execmode.equals("P"))
                release_process_locks(tid);
        }
        else if (instr.split("[ ,()]")[0].equals("I")){
            //stats.total_write_count += 1;
            run_write(instr, tid);
            if(activeTids.get(tid).execmode.equals("P"))
                release_process_locks(tid);
        } else if (instr.split("[ ,()]")[0].equals("U")){
            //stats.total_write_count += 1;
            run_update(instr, tid);
            if(activeTids.get(tid).execmode.equals("P"))
                release_process_locks(tid);
        
        }
        else if(instr.split("[ ,()]")[0].equals("M")){
            //stats.total_mread_count += 1;
            run_mread(instr, tid);
			if(activeTids.get(tid).execmode.equals("P"))
                release_process_locks(tid);
        }
        else if (instr.split("[ ,()]")[0].equals("C")) {
            //cout << "Running Delayed Commit for ID: " + to_string(txid) << endl; 
            if(activeTids.get(tid).execmode.equals("P"))
                release_process_locks(tid); 
            run_commit(instr, tid); 
        }
        else if (instr.split("[ ,()]")[0].equals("A")) {
            //cout << "Running Delayed Abort for ID: " + to_string(txid)  << endl;
            if(activeTids.get(tid).execmode.equals("P"))
                release_process_locks(tid); 
            run_abort(instr, tid); 
        }


    }

    void release_process_locks(String txid){
        TidData item = activeTids.get(txid);
        // Process should always only have one lock. 
        if(item.acquiredLocks.size()!=0 ){
            Lock lock = item.acquiredLocks.get(0); 
            String release_result = "";
            // if(lock.locktype.equals("read")) {
            //     release_result = lm.removeLock(lock.table, lock.userid, txid, "P-"); 
            // } else
            //  if (lock.locktype.equals("mread")) {
            //     release_result = lm.release_mlock(lock->table, lock->area_code, txid, "P"); 
            // } else 
            if (lock.locktype.equals("write")) {
                release_result = lm.removeLock(lock.table, lock.userid, txid, "P-WL"); 
               // writetime+=System.currentTimeMillis()-td.starttime;
                    totalwrites++;
            }
            item.acquiredLocks.remove(0) ; 
            activeTids.put(txid, item);
            canStartNewTxn(release_result);
        }
        
        
    }

    boolean acquireLock(String instr,String tid){

        if(tid.equals("-1")){//for begin
            return true;
        }
        for(String i: abortedTransactions){
            if(i.equals(tid+"")){
                //stats.abortscount++
                System.out.println("instr from aborted txn."+i+" returning");

                return false;
            }
        }
        String operation=instr.split("[ ,()]")[0];
        if(operation.equals("R")){
            String table = instr.split("[ ,()]")[1];

            TidData td=activeTids.get(tid);

            if(!td.waiton.equals("")){
                //transac delayed as it was waiting
                td.delayedOp.add(instr);
                activeTids.put(tid, td);
                return false;
            }
            if(td.execmode.equals("T")){
            if(lm.addlock(table, instr.split("[ ,()]")[2], tid,td.execmode+"-RL")){
                Lock l=new Lock();
                l.locktype="read";
                l.table=table;
                l.userid=instr.split("[ ,()]")[2];
                if(!lockInAcquired(tid,l)){
                    td.acquiredLocks.add(l);
                    activeTids.put(tid,td);
                }
                return true;
                }
                td.waiton=instr;
                activeTids.put(tid,td);
                return false;

            }else{
                return true;
            }
        }else if(operation.equals("I")||operation.equals("U")){
            String table = instr.split(" ")[1];

            TidData td=activeTids.get(tid);

            if(!td.waiton.equals("")){
                //transac delayed as it was waiting
                td.delayedOp.add(instr);
                activeTids.put(tid, td);
                return false;
            }

            if(lm.addlock(table, instr.trim().replaceAll("[(,)]+","").split(" ")[2], tid,td.execmode+"-WL")){
                Lock l=new Lock();
                l.locktype="write";
                l.table=table;
                String details = instr.trim().replaceAll("[(,)]+","");  
                l.userid=details.split(" ")[2];
                if(!lockInAcquired(tid,l)){
                    td.acquiredLocks.add(l);
                    activeTids.put(tid,td);
                }
                return true;
            }
                td.waiton=instr;
                activeTids.put(tid,td);
                return false;

        }else if(operation.equals("M")){
            String table = instr.split(" ")[1];

            TidData td=activeTids.get(tid);

            if(!td.waiton.equals("")){
                //transac delayed as it was waiting
                td.delayedOp.add(instr);
                activeTids.put(tid, td);
                return false;
            }
            if(td.execmode.equals("T")){
                if(lm.addlock(table, instr.split("[ ,()]")[2], tid,td.execmode+"-MREAD")){
                        Lock l=new Lock();
                        l.locktype="mread";
                        l.table=table;
                        l.userid=instr.split("[ ,()]")[2];
                        if(!lockInAcquired(tid,l)){
                            td.acquiredLocks.add(l);
                            activeTids.put(tid,td);
                        }
                        return true;
                    }
                td.waiton=instr;
                activeTids.put(tid,td);
                return false;

            }else{
                return true;
            }

        }else if(operation.equals("B")){
            return true;

        }else if(operation.equals("A")){
            detectdeadlock(instr);
            TidData td=activeTids.get(tid);
            if(td!=null){
            if(!td.waiton.equals("")){
                //transac delayed as it was waiting
                td.delayedOp.add(instr);
                activeTids.put(tid, td);
                return false;
            }}
            return true;

            
        }else if(operation.equals("C")){
            detectdeadlock(instr);
            TidData td=activeTids.get(tid);
            if(td!=null){
            if(!td.waiton.equals("")){
                //transac delayed as it was waiting
                System.out.println("delayed txn"+td.waiton);
                td.delayedOp.add(instr);
                activeTids.put(tid, td);
                return false;
            }
        }
        System.out.println("return true for commit lock");
            return true;
            
        }
        
        
        return false;
    }


    void run_read(String instr,String txid){
        String id = instr.split(" ")[2];
        String table_name="";
          
            table_name = instr.split(" ")[1];  
        if(sfs!=null){
           // System.out.println("called read in sfs");
        sfs.seqRead(table_name, Integer.parseInt(id));
        }else{
        lsm.readLSM(txid,table_name, Integer.parseInt(id));
        }
      
    }

    void run_write(String instr,String txid) {
        String table_name= instr.split(" ")[1];  
        String details = instr.substring(4).trim().replaceAll("[( )]+","");  
                System.out.println("called insert"+details);
                int user_id = Integer.parseInt(details.split(",")[0].trim());
                int user_age = Integer.parseInt(details.split(",")[1].trim());
                String sc = details.split(",")[2]; 
                System.out.print(details);
                //sc=sc.substring(0,sc.length()-1); 
                byte satisfaction_score = Byte.parseByte(sc);

                Record record = new Record(user_id, user_age, satisfaction_score);
                if(sfs!=null){
                sfs.seqWrite(txid,table_name, record);}
                else{
                lsm.writeLSM(txid,table_name, record);
                }
    }

    void run_mread(String instr,String txid){
        String table_name= instr.split(" ")[1];  
        String option = instr.split(" ")[2];
                String score = instr.split(" ")[3]; 
                int age = Integer.parseInt(score);           
                Boolean selected_option = true;
                if(option.equals("a")){                                     
                    score = "0";
                }else if(option.equals("s")){
                    selected_option = false;
                    age = 0;
                }
                if(sfs!=null){
                sfs.seqMRead(table_name, selected_option, age, Byte.parseByte(score));
                }else{
                lsm.mreadLSM(txid,table_name, selected_option, age, Byte.parseByte(score));
                }
    }

    void run_update(String instr,String txid){
        String table_name= instr.split(" ")[1];  
        int id = Integer.parseInt(instr.split(" ")[2]);
        String sc = instr.split(" ")[3]; 
        //sc=sc.substring(0,sc.length()-1);      
        Byte score = Byte.parseByte(sc);
        if(sfs!=null){
        sfs.seqUpdate(txid,table_name, id, score);}
        else{
        lsm.updateLSM(txid,table_name, id, score);
        }
    }

    void run_abort(String line, String txid)
    {
        // Call Abort at DM first!
        rm.abort(txid);
        Logging.logAbort(txid);
        if (!activeTids.get(txid).waiton.equals(""))
    		release_waiting_locks(txid);
        releaseLocks(txid, true);
    }
    void release_waiting_locks(String tid){

        TidData td=activeTids.get(tid);
        String[] waitops=td.waiton.split("[ ,()]");
        String table=waitops[1];
        td.waiton="";
        //stats.updateabortcount

        if(waitops[0].equals("R")){
            if(td.execmode.equals("T"))
                lm.removeLock(table, waitops[2], tid, td.execmode+"-RL");
                readtime+=System.currentTimeMillis()-td.starttime;
                    totalreads++;

        }else if(waitops[0].equals("I")){
            lm.removeLock(table, waitops[2], tid, td.execmode+"-WL");
            writetime+=System.currentTimeMillis()-td.starttime;
                    totalwrites++;

        }else if(waitops[0].equals("M")){
            if(td.execmode.equals("T"))
            lm.removeLock(table, waitops[2], tid, td.execmode+"-MREAD");
            readtime+=System.currentTimeMillis()-td.starttime;
                    totalreads++;


        }else if(waitops[0].equals("U")){
            lm.removeLock(table, waitops[2], tid, td.execmode+"-WL");
            writetime+=System.currentTimeMillis()-td.starttime;
                    totalwrites++;

        }

    }

    void run_commit(String line, String txid)
    {
        System.out.println("run commit for:"+txid);
        for(String i: abortedTransactions){
        if(i.equals(txid+"")){
            //stats.abortscount++
            System.out.println("instr from aborted txn."+i+" returning");
            return;
        }
    }
        rm.commit(txid);
        Logging.logCommit(txid);
        totalcommits+=1;
        releaseLocks(txid, false);
    }
    void run_begin(String line, String txid)
    {
        if(line.split("[ ,()]")[1].equals("0")) {
            Logging.logBegin(txid);

            activeTids.put(txid,new TidData("P"));
        }
        else{
            Logging.logBegin(txid);
            rm.begin(txid);
        activeTids.put(txid,new TidData("T"));
        }
        
    }


    
}
