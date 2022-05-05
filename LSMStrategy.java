import java.io.*;
import java.util.*;
public class LSMStrategy{
    public int sstablecapacity;//number of diskblocks
    public LRUBuffer cache;
	public int cachecapacity;//int bytes. multiple of diskblocksize
	public static int cacheind=0;
    public int diskblocksize;//in bytes.multiple of 12
    public int levels;
    public HashMap<String,TreeMap<Integer,Record>> memtablemap;
    public HashMap<String,Integer> tableNum;
    public int tablesinlevelzero;
    public HashMap<String,gnode> gOpMap;
    public int finallevel=0;

    ArrayList<String> tablenames=new ArrayList<String>();
    public class gnode{
		public byte min;
		public byte max;
		public double avg;
		public int count=0;
	}
    public LSMStrategy(int sstablecapacity,int cachecapacity,int tablesinlevelzero,int diskblocksize){
        this.sstablecapacity=sstablecapacity;
        this.cachecapacity=cachecapacity;
        this.tablesinlevelzero=tablesinlevelzero;
        this.diskblocksize=diskblocksize;
        this.cache = new LRUBuffer((int)cachecapacity/diskblocksize);//number of disk blocks that a cache can store
        memtablemap=new HashMap<String,TreeMap<Integer,Record>>();
        tableNum=new HashMap<String,Integer>();
        this.levels=0;
        this.gOpMap=new HashMap<String,gnode>();

        while(tablesinlevelzero!=0){
            tablesinlevelzero=(int)(tablesinlevelzero/10);
            this.finallevel+=1;
        }

    }

    public double GLSM(String table,int op){
		double ans=0.0;
		gnode g=gOpMap.get(table);
        if(g==null){
			return -1;
		}
		if(op==0){//min
			ans=g.min;
			Logging.logG(table,"MIN",ans);

		}else if(op==1){//max
			ans=g.max;
			Logging.logG(table,"MAX",ans);

		}else{//avg
			ans=g.avg;
			Logging.logG(table,"AVG",ans);
		}
		return ans;
	}

    public void checkIfMemtableIsFull(String tid,String table,TreeMap<Integer,Record> memtable){

        if(memtable.size()<(int)(sstablecapacity*diskblocksize/12)){
            memtablemap.put(table,memtable);
            return;
        }
        memtablemap.remove(table);
        //need to flush memtable.
        //before that, check if level 0 is full.
        try{
            File dir=new File("lsmtables");
            if (! dir.exists()){
                dir.mkdir();
            }
            String[] files=dir.list();
            int nooffiles=0;
            for(String fname:files){
                if(fname.substring(1,fname.indexOf(table)).equals("0")){
                    nooffiles+=1;
                }
            }
            //System.out.print("number of files:"+nooffiles+"\n");
            if(nooffiles>=tablesinlevelzero){
                tableNum.clear();
                startCompaction(tid,0);//todo: write compaction code
            }
            //tableNum is a map that has the number of sstables count for the respective table name
            if(!tableNum.containsKey(table)){
                tableNum.put(table,1);
            }else{
                tableNum.put(table,tableNum.get(table)+1);
            }
            FileOutputStream ff = new FileOutputStream("lsmtables/L0"+table+tableNum.get(table) + ".txt");
            ObjectOutputStream outstr = new ObjectOutputStream(ff);
            TreeMap<Integer,Record> block=new TreeMap<Integer,Record>();

            ArrayList<TreeMap<Integer,Record>> diskblockslist=new ArrayList<TreeMap<Integer,Record>>();

            for(Object key:memtable.keySet()){
                Record re=(Record)memtable.get((int)key);
                if(block.size()<(int)(diskblocksize/12)){
                    block.put((int)key,re);
                }else{
                   // System.out.println("block length before inserting to file"+block.size());
                    diskblockslist.add(block);
                    block=new TreeMap<Integer,Record>();
                    block.put((int)key,re);
                }
            }
            diskblockslist.add(block);
            outstr.writeObject(diskblockslist);
            outstr.close();
            ff.close();
            String key1=memtable.firstKey()+"";
            String key2=memtable.lastKey()+"";
            Logging.logCreateLSM(tid,0,table,Integer.parseInt(key1),table,Integer.parseInt(key2));



        }catch(Exception e){
            System.out.println("can't open file error in lsm check\n"+e);
        }


    }

    public void writeLSM(String tid,String table,Record rec){
        //should it be set in cache??
        TreeMap<Integer,Record> memtable;
        if(!tablenames.contains(table)){
            tablenames.add(table);
            gnode g=new gnode();
            g.min=5;
            g.max=0;
            g.avg=0;
            g.count=0;
            gOpMap.put(table,g);
        }else{
				gnode g=gOpMap.get(table);
				if(rec.satisfaction_score<g.min){
					g.min=rec.satisfaction_score;
				}else if(rec.satisfaction_score>g.max){
					g.max=rec.satisfaction_score;
				}
				g.avg=(g.avg*g.count+rec.satisfaction_score)/(g.count+1);
				g.count+=1;
				gOpMap.put(table,g);
			}
        if(memtablemap.containsKey(table)){
            memtable=memtablemap.get(table);
        }else{
            memtable=new TreeMap<Integer,Record>();
        }
        memtable.put(rec.user_id,rec);
        checkIfMemtableIsFull(tid,table,memtable);
        Logging.logWrite(tid,table,rec.user_id,rec.user_age,rec.satisfaction_score);

    }

    public ArrayList<Record> mreadLSM(String tid,String table,boolean checkage,int age,byte satisfaction_score ){
        ArrayList<Record> ans=new ArrayList<Record>();
        try{
            if(checkage){
            Logging.logOperation(tid+" M "+table+" a "+age+"\n");}
            else{
                Logging.logOperation(tid+" M "+table+" s "+satisfaction_score+"\n");
            }
            //check the memtable and then sstables in all the levels
            TreeMap<Integer,Record> memtable=memtablemap.get(table);
            HashSet<Integer> uniquek=new HashSet<Integer>();
            if(memtable!=null){
                //consider the multiple keys case due to update
                for(Object key:memtable.keySet()){
                    if(uniquek.contains((int)key)==true){
                        continue;
                    }else{
                        Record re=(Record)memtable.get(key);
                        uniquek.add((int)key);
                        if(checkage){
                        if(re.user_age==age){
                            //uniquek.add((int)key);
                            ans.add((Record)memtable.get(key));
                            Logging.logMRead(table,re.user_id,re.user_age,re.satisfaction_score);

                        }  
                        }else{
                        if(re.satisfaction_score==satisfaction_score){
                            ans.add(re);
                            Logging.logMRead(table,re.user_id,re.user_age,re.satisfaction_score);

                        } 
                        }
                    }
                }
            }

            int level=0;
            while(level<=levels){
                //search in the SSTables at each level starting from 0
                //at each level, start from the latest file.
                File dir=new File("lsmtables");
                String[] fnames=dir.list();
                int nooffiles=0;
                for(String fname:fnames){
                    if(fname.substring(1,fname.indexOf(table)).equals(String.valueOf(level))){
                        nooffiles+=1;
                    }
                }
                for(int x=nooffiles;x>0;x--){
                    String fname="L"+String.valueOf(level)+table+String.valueOf(x)+".txt";
                    //System.out.print("file name:"+fname);
                    FileInputStream fis = new FileInputStream("lsmtables/"+fname);
                    ObjectInputStream outstr = new ObjectInputStream(fis);
                    ArrayList<TreeMap<Integer,Record>> diskblocklist;
                    diskblocklist = (ArrayList<TreeMap<Integer,Record>>)outstr.readObject();
                    TreeMap<Integer,Record> treeMap;
                    for(int h=0;h<diskblocklist.size();h++){
                        treeMap=diskblocklist.get(h);
                        //System.out.print("\n mread: disk block for cache length"+treeMap.size());
                        cache.addlsm(table,level,treeMap);                    
                        for(Integer kk: treeMap.keySet()){
                            if(uniquek.contains((int)kk)){continue;}
                            Record rec=treeMap.get(kk);
                            uniquek.add(kk);
                            if(checkage){
                            if(rec.user_age==age){
                                ans.add(rec);
                                Logging.logMRead(table,rec.user_id,rec.user_age,rec.satisfaction_score);

                            }  
                            }else{
                            if(rec.satisfaction_score==satisfaction_score){
                                ans.add(rec);
                                //uniquek.add(kk);
                                Logging.logMRead(table,rec.user_id,rec.user_age,rec.satisfaction_score);
                            
                            } 
                            }

                        }
                    }
                    outstr.close();
                    fis.close();

                }
                level++;
            }

        }catch(Exception e){
            System.out.println("\ncan't open file in mread lsm"+e);
        }
        return ans;

        
    }

    public Record readLSM(String tid,String table,int user_id){
        //first search in cache
        //if not in cache, then search in memtable
        //else search in the SSTables at each level starting from 0
        Logging.logOperation("R "+table+" "+user_id+"\n");
        try{
            Record cacherec=cache.getlsm(table,user_id);
            if(cacherec!=null){
                if(cacherec.satisfaction_score==0){
                    return null;
                }
                Logging.logRead(table,user_id,cacherec.user_age,cacherec.satisfaction_score);
                return cacherec;

            }
            TreeMap<Integer,Record> memtable=memtablemap.get(table);
            if(memtable!=null){
                if(memtable.get(user_id)!=null){
                    Record rr=(Record)memtable.get(user_id);
                    if(rr.satisfaction_score==0){
                        return null;
                    }
                    Logging.logRead(table,user_id,rr.user_age,rr.satisfaction_score);
                    return rr;
                }
            }
            int level=0;
            while(level<=finallevel){
                //search in the SSTables at each level starting from 0
                //at each level, start from the latest file.
                File dir=new File("lsmtables");
                if (! dir.exists()){
                    dir.mkdir();
               
                }
                String[] fnames=dir.list();
                int nooffiles=0;
                for(String fname:fnames){
                    if(fname.substring(1,fname.indexOf(table)).equals(String.valueOf(level))){
                        nooffiles+=1;
                    }
                }
                for(int x=nooffiles;x>0;x--){
                    String fname="L"+String.valueOf(level)+table+String.valueOf(x)+".txt";
                    System.out.print(fname);
                    FileInputStream fis = new FileInputStream("lsmtables/"+fname);
                    ObjectInputStream outstr = new ObjectInputStream(fis);
                    ArrayList<TreeMap<Integer,Record>> diskblocklist;
                    diskblocklist = (ArrayList<TreeMap<Integer,Record>>)outstr.readObject();
                    TreeMap<Integer,Record> treeMap;
                    outstr.close();
                    fis.close();
                    for(int h=0;h<diskblocklist.size();h++){
                        treeMap=diskblocklist.get(h);
                        //System.out.print("\n disk block for cache length"+treeMap.size());
                        cache.addlsm(table,level,treeMap);
                        if(treeMap.get(user_id)!=null){
                            Record ans=(Record)treeMap.get(user_id);
                            if(ans.satisfaction_score==0){
                                return null;
                            }
                            Logging.logRead(table,user_id,ans.user_age,ans.satisfaction_score);
                            return ans;
    
                        }
                    }


                }


                level++;
            }
        }catch(Exception e){
            System.out.println("could not open file in readlsm because lsmtables dir is missing\n");

        }
        return null;//aborted
    }

    public void updateLSM(String tid,String table,int user_id,byte val){
        //delete the record and insert new updated record.
        TreeMap<Integer,Record> memtable;
        if(memtablemap.containsKey(table)){
            memtable=memtablemap.get(table);
        }else{
            memtable=new TreeMap<Integer,Record>();
        }
        gnode g=gOpMap.get(table);
			if(val<g.min){
				g.min=val;
			}else if(val>g.max){
				g.max=val;
			}
			
        // Record del=new Record(-1,-1,Byte.parseByte(1));
        // memtable.put(user_id,del);
        // checkIfMemtableIsFull(table,memtable);

        Record r=readLSM(tid,table,user_id);
        // if(memtablemap.containsKey(table)){
        //     memtable=memtablemap.get(table);
        // }else{
        //     memtable=new TreeMap<Integer,Record>();
        // }
        if(r==null){
            return;
        }
        byte prev=r.satisfaction_score;
        g.avg=(g.avg*g.count+val-prev)/(g.count);
			//g.count+=1;
			gOpMap.put(table,g);
        r.satisfaction_score=val;
        memtable.put(user_id,r);
        cache.putlsm(table,r);//todo: write new cache code // this block was in cache,so it is updated
        //todo: should i??? write through cache update the .txt file also
        checkIfMemtableIsFull(tid,table,memtable);
        Logging.logUpdate(tid,table,user_id,r.user_age,val,prev);


    }
    public int isEmpty(int level){
        try{
            File dir=new File("lsmtables");
            String[] fnames=dir.list();
            for(String fname:fnames){
                if(fname.substring(1,(level+"").length()+1).equals(String.valueOf(level))){
                    return 0;
                }
            }
        }catch(Exception e){
            System.out.println("error opening files is empty\n"+e);
        }
        return 1;
    }

    public int levelIsFull(int level){
        if(level>finallevel && finallevel!=-1){
            return 0;
        }
       // System.out.println("\n******************************************************************************************************\n");
        try{
            File dir=new File("lsmtables");
            String[] fnames=dir.list();
            int nooffiles=0;
            for(String fname:fnames){
                if(fname.substring(1,(level+"").length()+1).equals(String.valueOf(level))){
                    nooffiles+=1;
                }
            }
            if(level==0){
                if(nooffiles>=tablesinlevelzero){
                    return 1;
                }
                return 0;
            }
            if(nooffiles>=(int)(tablesinlevelzero/Math.pow(10,level))){
                return 1;
            }
        }catch(Exception e){
            System.out.println("error opening files level is full\n");
        }
        return 0;
    }

    public TreeMap<Integer,Record> getfiles(String table,int levelno, TreeMap<Integer,Record> merge){
        try{
        TreeMap<Integer,Record> treeMap=new TreeMap<Integer,Record>();
        File dir=new File("lsmtables");
        String[] fnames=dir.list();
        for(String fname:fnames){
            //System.out.println("\nentered loop\n"+fname+levelno+table);
            if(fname.contains("L"+levelno+table)){//levelnumber and tablename match
                //System.out.println("entered:"+fname);
                FileInputStream fis = new FileInputStream("lsmtables/"+fname);
                ObjectInputStream outstr = new ObjectInputStream(fis);
                ArrayList<TreeMap<Integer,Record>> diskblocklist;
                diskblocklist = (ArrayList<TreeMap<Integer,Record>>)outstr.readObject();
                for(int h=0;h<diskblocklist.size();h++){
                    treeMap=diskblocklist.get(h);
                    for (Integer key : treeMap.keySet()) {
                        merge.put(key, treeMap.get(key));
                    }
                }
                outstr.close();
                fis.close();
                File ff=new File("lsmtables/"+fname);
                //System.out.println("deleting file:"+fname);
                ff.delete(); 
            
            }
        }
    }catch(Exception e){
        System.out.println("error opening file in getfiles func.lsm\n");
    }
    return merge;
    }

    public void startCompaction(String tid,int level){
        if(level==finallevel){
            return;
        }
        //System.out.println("compact func called\n");
        int nextlevel=level+1;
        try{
            File dir = new File("lsmtables");
            ArrayList<String> table = new ArrayList<>();
            String[] files=dir.list();
            for(String fname:files){
                if(fname.substring(1,(level+"").length()+1).equals(level+"")){
                    if (!table.contains(fname.substring(2, 3))) {
                        table.add(fname.substring(2, 3));
                    }
                }
            }
            int i=0;
            while(i<table.size()){
                //foreach table
                int nrecs = 0;//number of records
                int count = 1;//sstable number in that level
                TreeMap<Integer,Record> merge = new TreeMap<Integer,Record>();
                if(isEmpty(nextlevel)==1 && nextlevel!=finallevel)
                {
                    levels+=1;
                }else
                {
                   merge= getfiles(table.get(i),nextlevel,merge);
                   if(merge.size()>0){

                    Logging.logSwapLSM(true,level,table.get(i),Integer.parseInt(merge.firstKey()+""),table.get(i),Integer.parseInt(merge.lastKey()+""));

                   }
                }
               // System.out.println("compact func called: levels is"+levels);
                merge=getfiles(table.get(i),level,merge);//will have all the compacted records
                //create a new table if the merged length is >sstablecapacity.
                TreeMap<Integer,Record> newssttable = new TreeMap<Integer, Record>();
                Iterator<Integer> itr = merge.keySet().iterator();
                //System.out.println("\ncompact: merge size is"+merge.size());
                if(nextlevel==finallevel){
                    System.out.println("entered final level\n"+nextlevel);
                    FileOutputStream fis = new FileOutputStream("lsmtables/L" + nextlevel + table.get(i) + count + ".txt");
                        ObjectOutputStream outstr = new ObjectOutputStream(fis);
                        ArrayList<TreeMap<Integer,Record>> diskblockslist=new ArrayList<TreeMap<Integer,Record>>();
                        TreeMap<Integer,Record> block=new TreeMap<Integer,Record>();
                        for(Integer key1:merge.keySet()){
                            Record re=(Record)merge.get((int)key1);
                            if(block.size()<(int)(diskblocksize/12)){
                                block.put((int)key1,re);
                            }else{
                                diskblockslist.add(block);
                                block=new TreeMap<Integer,Record>();
                                block.put((int)key1,re);
                            }
                        }
                        diskblockslist.add(block);
                        outstr.writeObject(diskblockslist);
                        outstr.close();
                        fis.close();
                    


                }else{
                while (itr.hasNext()) {
                    Integer key = itr.next();
                    newssttable.put(key, (Record)merge.get(key));
                    if (nrecs == (int)(sstablecapacity*(level+1)*diskblocksize/12)) {
                        Logging.logCreateLSM(tid,nextlevel,table.get(i),Integer.parseInt(newssttable.firstKey()+""),table.get(i),Integer.parseInt(newssttable.lastKey()+""));
                        FileOutputStream fis = new FileOutputStream("lsmtables/L" + nextlevel + table.get(i) + count + ".txt");
                        ObjectOutputStream outstr = new ObjectOutputStream(fis);
                        ArrayList<TreeMap<Integer,Record>> diskblockslist=new ArrayList<TreeMap<Integer,Record>>();
                        TreeMap<Integer,Record> block=new TreeMap<Integer,Record>();
                        for(Integer key1:newssttable.keySet()){
                            Record re=(Record)newssttable.get((int)key1);
                            if(block.size()<(int)(diskblocksize/12)){
                                block.put((int)key1,re);
                            }else{
                                diskblockslist.add(block);
                                block=new TreeMap<Integer,Record>();
                                block.put((int)key1,re);
                            }
                        }
                        diskblockslist.add(block);
                        outstr.writeObject(diskblockslist);
                        //outstr.writeObject(newssttable);
                        newssttable = new TreeMap<Integer, Record>();
                        outstr.close();
                        fis.close();
                        nrecs = 0;
                        count++;
                    }
                    nrecs=nrecs+1;
                }
                if(nrecs>=1){
                    Logging.logCreateLSM(tid,nextlevel,table.get(i),Integer.parseInt(newssttable.firstKey()+""),table.get(i),Integer.parseInt(newssttable.lastKey()+""));
                    FileOutputStream fis = new FileOutputStream("lsmtables/L" + nextlevel + table.get(i) + count + ".txt");
                    ObjectOutputStream outstr = new ObjectOutputStream(fis);
                    ArrayList<TreeMap<Integer,Record>> diskblockslist=new ArrayList<TreeMap<Integer,Record>>();
                    TreeMap<Integer,Record> block=new TreeMap<Integer,Record>();
                    for(Integer key1:newssttable.keySet()){
                        Record re=(Record)newssttable.get((int)key1);
                        if(block.size()<(int)(diskblocksize/12)){
                            block.put((int)key1,re);
                        }else{
                            diskblockslist.add(block);
                            block=new TreeMap<Integer,Record>();
                            block.put((int)key1,re);
                        }
                    }
                    diskblockslist.add(block);
                    outstr.writeObject(diskblockslist);
                    //outstr.writeObject(newssttable);
                    newssttable = new TreeMap<Integer, Record>();
                    outstr.close();
                    fis.close();
                    nrecs = 0;
                    count++;

                }}
                i++;

            }
            if(levelIsFull(nextlevel)==1)
            {//recursively merge the next levels
                startCompaction(tid,nextlevel);
            }
        }catch(Exception e){
            System.out.println("error opening files\n"+e);
        }
    }

   // public static void main(String args[]){

        // //disckblocksize must be a multiple of 12
        // LSMStrategy lsmo=new LSMStrategy(2, 2, 2, 1);
        // //2 records per sstable
        // //2 blocks i.e 2 sstables per cache
        // //2 tables in level zero

        // Record r=new Record(1,10,(byte)1);
        // LSMStrategy lsmo=new LSMStrategy(2, 2*24, 2, 24);
        //sstable can hold 2 disk blocks i.e 4 records = memtable too
        //cache can hold 2 disk blocks i.e 4 records
        //tables in level zero is 2
        //diskblock size is 24bytes so it can hold 2 records
        // lsmo.writeLSM("1","X", r);
        // lsmo.readLSM("1","X", 1);
        // r=new Record(2,20,(byte)1);
        // lsmo.writeLSM("1","X", r);
        // r=new Record(3,30,(byte)1);
        // lsmo.writeLSM("1","X", r);
        // r=new Record(4,40,(byte)2);
        // lsmo.writeLSM("1","X", r);
        // lsmo.updateLSM("1","X", 1, (byte)3);
        // r=new Record(5,50,(byte)1);
        // lsmo.writeLSM("1","X", r);
        // r=new Record(6,60,(byte)2);
        // lsmo.writeLSM("1","X", r);
        // r=new Record(7,70,(byte)1);
        // lsmo.writeLSM("1","X", r);
        // r=new Record(8,80,(byte)1);
        // lsmo.writeLSM("1","X", r);
        // lsmo.mreadLSM("1","X",true,70,Byte.parseByte(0+""));
        // lsmo.mreadLSM("1","X",false,0,Byte.parseByte(2+""));
        // lsmo.GLSM("1","X", 0);

        // lsmo.updateLSM("1","X", 8, (byte)4);
    //     r=new Record(9,80,(byte)1);
    //     lsmo.writeLSM("1","X", r);
    //     r=new Record(10,80,(byte)1);
    //     lsmo.writeLSM("1","X", r);
    //     r=new Record(11,80,(byte)1);
    //     lsmo.writeLSM("1","X", r);
    //     lsmo.readLSM("1","X", 2);
    //     r=new Record(12,80,(byte)1);
    //     lsmo.writeLSM("1","X", r);
    //     r=new Record(13,80,(byte)1);
    //     lsmo.writeLSM("1","X", r);
    //     lsmo.readLSM("1","X", 9);
    //     lsmo.readLSM("1","X", 12);
    //     lsmo.readLSM("1","X", 13);
    //     lsmo.readLSM("1","X", 3);
    //     lsmo.readLSM("1","X", 6);
    //    lsmo.readLSM("1","X", 9);
    //     lsmo.readLSM("1","X", 13);

  //  }


}