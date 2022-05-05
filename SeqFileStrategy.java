//import java.io.BufferedReader;
import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedHashMap;
import java.util.ArrayList;
//import java.util.Iterator;
import java.util.StringTokenizer;
//import java.io.FileWriter;
import java.io.RandomAccessFile;


// This file strategy stores output in log.txt and inserts data into tablename.txt. example X.txt
//to do: remove the B+ trees code
//we use writethrough cache
public class SeqFileStrategy {
	public int diskblocksize; // in bytes. also called as pagesize
	public int cachecapacity;//in bytes.
	public static int cacheind=0;
	public LRUBuffer cache;
	public HashMap<String,HashMap<Integer,PageTableRecord>> pagetable;
	//public HashMap<String,BplusTree> useridtrees;
	public HashMap<String,ISAMlist> isamMapForEachTable;
	public HashMap<String,gnode> gOpMap;
	public class gnode{
		public byte min;
		public byte max;
		public double avg;
		public int count=0;
	}
	public SeqFileStrategy(int diskblocksize,int cachecapacity){
		//cacheindex=0;
		this.diskblocksize=diskblocksize;
		this.cachecapacity=cachecapacity;
		this.pagetable=new HashMap<String,HashMap<Integer,PageTableRecord>>();
		this.cache = new LRUBuffer((int)cachecapacity/diskblocksize);//number of disk blocks that a cache can store
		//this.useridtrees=new HashMap<String,BplusTree>();
		this.isamMapForEachTable=new HashMap<String,ISAMlist>();
		this.gOpMap=new HashMap<String,gnode>();
	}

	public double seqG(String table,int op){
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

	public Record seqRead(String table,int user_id){
		Logging.logOperation("R "+table+" "+user_id+"\n");
		//abort if table does not exist
		File f=new File(table+".txt");
    	if(!f.exists()){
      		return null;//aborted.
    	}
		// use the  ISAM to find the pagenumber. if not found, then return false
		ISAMlist iasaml=isamMapForEachTable.get(table);
		if(iasaml==null){//if you try to read before inserting.
			return null;
		}
		int indd=iasaml.read(user_id);
		int pageno=iasaml.accesslist.get(indd).blockno;
		if(pageno==-1)return null;
		//System.out.println("\n pageno to read is"+pageno);
		int cacheindex=swapIn(f,table,pageno);
		//System.out.println("\n cacheindex is"+cacheindex);
		CacheNode cn=cache.get(cacheindex);
		//System.out.println("THE data read is "+cn.data);
		StringTokenizer st1=new StringTokenizer(cn.data,";");
		byte b=Byte.parseByte("0");
		int a=0;
		int u=0;
		while(st1.hasMoreTokens()){
			//String rec=st1.nextToken();
			StringTokenizer st =new StringTokenizer(st1.nextToken(),",");
			String s=st.nextToken().trim();
			 u=Integer.parseInt(s);
			if(u==user_id){
				s=st.nextToken().trim();
				 a=Integer.parseInt(s);
				s=st.nextToken();
				 b=Byte.parseByte(s);
				break;
			}
		}
		Record ans=new Record(u,a,b);
		Logging.logRead(table,ans.user_id,ans.user_age,ans.satisfaction_score);
		//System.out.println("finished read\n");
		return ans;

	}

	// public Record seqRead(String table,int user_id){
	// 	//abort if table does not exist
	// 	File f=new File(table+".txt");
    // 	if(!f.exists()){
    //   		return null;//aborted.
    // 	}
	// 	// use the bplus tree to find the pagenumber. if not found, then return false
	// 	int pageno=useridtrees.get(table).search(user_id);
	// 	if(pageno==-1)return null;
	// 	//System.out.println("\n pageno to read is"+pageno);
	// 	int cacheindex=swapIn(f,table,pageno);
	// 	System.out.println("\n cacheindex is"+cacheindex);
	// 	CacheNode cn=cache.get(cacheindex);
	// 	System.out.println("THE data read is "+cn.data);
	// 	StringTokenizer st1=new StringTokenizer(cn.data,";");
	// 	byte b=Byte.parseByte("0");
	// 	int a=0;
	// 	int u=0;
	// 	while(st1.hasMoreTokens()){
	// 		//String rec=st1.nextToken();
	// 		StringTokenizer st =new StringTokenizer(st1.nextToken(),",");
	// 		String s=st.nextToken().trim();
	// 		 u=Integer.parseInt(s);
	// 		if(u==user_id){
	// 			s=st.nextToken().trim();
	// 			 a=Integer.parseInt(s);
	// 			s=st.nextToken();
	// 			 b=Byte.parseByte(s);
	// 			break;
	// 		}
	// 	}
	// 	Record ans=new Record(u,a,b);
	// 	Logging.logRead(table,ans.user_id,ans.user_age,ans.satisfaction_score);
	// 	return ans;

	// }

	public ArrayList<Record> seqMRead(String table,boolean checkage,int age,byte satisfaction_score ){
		//if not check age then check satisfaction score
		//abort if table does not exist
		if(checkage){
		Logging.logOperation("M "+table+" a "+age+"\n");}
		else{
			Logging.logOperation("M "+table+" s "+satisfaction_score+"\n");
		}
		File f=new File(table+".txt");
		if(!f.exists()){
		return null;//aborted.
		}
		ArrayList<Record> ans=new ArrayList<Record>();
		int numberofpages=pagetable.get(table).size();
		int i,index;
		CacheNode cn;
		for(i=0;i<numberofpages;i++){//for each page/block
			index=swapIn(f,table,i);
			cn=cache.get(index);
			StringTokenizer st = new StringTokenizer(cn.data,";\n");
			while(st.hasMoreTokens()){//for each record
				String rec=st.nextToken();
				StringTokenizer stForID =new StringTokenizer(rec,",");
				int userid=Integer.parseInt(stForID.nextToken());//for user id
				if(checkage){//can shift this if block upwards to improve performance
					if(age==Integer.parseInt(stForID.nextToken())){
						Byte ss=Byte.parseByte(stForID.nextToken());

						ans.add(new Record(userid,age,ss));
						Logging.logMRead(table,userid,age,ss);
					}
				}else{
					int agee=Integer.parseInt(stForID.nextToken());
					if(satisfaction_score==Byte.parseByte(stForID.nextToken())){
						ans.add(new Record(userid,agee,satisfaction_score));
						Logging.logMRead(table,userid,agee,satisfaction_score);
					}
				}
			}

		}
		return ans;


	}

	public boolean seqUpdate(String tid,String table,int userid,byte val2){
		//return success or aborted the operation
		try{
		Logging.logOperation("U "+table+" "+userid+" "+val2+"\n");
		File f=new File(table+".txt");
		if(!f.exists()){
			return false;//aborted.
		}
		else{
			gnode g=gOpMap.get(table);
			if(val2<g.min){
				g.min=val2;
			}else if(val2>g.max){
				g.max=val2;
			}
			g.avg=(g.avg*g.count+val2)/(g.count+1);
			g.count+=1;
			gOpMap.put(table,g);
		}
		ISAMlist iasaml=isamMapForEachTable.get(table);
		int indd=iasaml.read(userid);
		int pageno=iasaml.accesslist.get(indd).blockno;
		if(pageno==-1)return false;
		int cacheindex=swapIn(f,table,pageno);
		CacheNode cn=cache.get(cacheindex);
		StringTokenizer st = new StringTokenizer(cn.data,";\n");
		int offset=0;
		String newrow="";
		int oldrowlen=0;
		byte prev=val2;
		int age=0;
		while(st.hasMoreTokens()){
			String rec=st.nextToken();
			StringTokenizer stForID =new StringTokenizer(rec,",");
			if(userid==Integer.parseInt(stForID.nextToken())){
				oldrowlen=rec.length();
				age=Integer.parseInt(stForID.nextToken());
				newrow=userid+","+age+","+val2;
				prev=Byte.parseByte(stForID.nextToken());
				//System.out.println("\nthe new row for update is:"+newrow);
				break;
			}
			offset+=rec.length()+2;//plus 2 for the semicolon and \n
		}
		//System.out.println("\noffset is:"+offset);
		String newpage=cn.data.substring(0,offset)+newrow+cn.data.substring(offset+1+oldrowlen);
		cache.put(cacheindex,new CacheNode(table,pageno,newpage));
		//System.out.println("\nNEW UPDATED disk block is :"+newpage);
		//if user is found, then update the page and ()
		RandomAccessFile ra_f = new RandomAccessFile(f, "rw");
		long pos=(pageno)*diskblocksize+1;
		// if(ra_f.length()-diskblocksize>=0)
		// {
		// 	pos=ra_f.length()-diskblocksize;
		// }

		ra_f.seek(pos);
		ra_f.write(newpage.getBytes());
		ra_f.close();
		Logging.logUpdate(tid,table,userid,age,val2,prev);
		return true;
		}catch(Exception e){
			System.out.println("exception in seqUpdate\n");
			return false;
		}

	}
	// public float seqG(String table,string op){
	// 	//abort if table does not exist
	//store the min,max,avg for each page while inserting the records

	// }

	public String readatfile(File f, int pageno)
	{
		try{
		RandomAccessFile ra_f = new RandomAccessFile(f, "r");

		byte[] dest = new byte[diskblocksize+1];
		int offset = (pageno)*diskblocksize+1;
		//System.out.println("read file from offset"+offset);
		ra_f.seek(offset);
		int bytesRead = ra_f.read(dest, 0, diskblocksize);//dest,offset,length
		ra_f.close();
		if(bytesRead!=0){
			String s=new String(dest);
			//System.out.println("read file reads string:"+s);
			return s.trim();
		}
		return "";
		}catch(Exception e){
			//System.out.println("exception in readatfile\n"+e);
			return "";
		}

	}

	public int swapIn(File f, String table, int pageno){
		//check if this page is already in cache
		//System.out.println("SWAP IN:"+table+pageno);
		if(pagetable.get(table).get(pageno)!=null){
			//System.out.println("ENTERED IF\n");
			if(pagetable.get(table).get(pageno).cacheindex!=-1){
				//it is in cache.
							//System.out.println("ENTERED IF bcz its in cache\n");
				return pagetable.get(table).get(pageno).cacheindex;
			}
		}

		//if the page is not in cache, read the page from file and put it into the cache
		cacheind+=1;
		String page=readatfile(f,pageno);

		CacheNode cn=new CacheNode(table,pageno,page);
		CacheNode replacedpage=cache.put(cacheind,cn);
		if(replacedpage!=null){ //if cache was full and a page was replaced, then update pagetable's index with -1
		pagetable.get(replacedpage.table).get(replacedpage.pageno).cacheindex=-1;
		Logging.logSwapSeq(false,replacedpage.table,replacedpage.pageno);
		}
					Logging.logSwapSeq(true,table,pageno);
		return cacheind;
	}

	public void writetofile(File f,String newpage,String table)
	{
		try{
			// FileWriter fw=new FileWriter(f,true);
			// fw.write(newpage);
			// fw.close();
			int pagesinfile=pagetable.get(table).size();
		RandomAccessFile ra_f = new RandomAccessFile(f, "rw");
		long pos=(pagesinfile)*diskblocksize+1;
		//System.out.println("\nwrite pos is "+pos);
		ra_f.seek(pos);
		if(diskblocksize-newpage.getBytes().length!=0){
			//padding
			StringBuffer str_bfr = new StringBuffer();
		for(int i=0;i<(diskblocksize-newpage.getBytes().length);i++)
		{
			//append string to stringbuffer n times
			str_bfr.append(" ");
		}
		//converting stringbuffer back to string using toString() method
		String str = str_bfr.toString();
			newpage+=str;
		}
		//System.out.println(" new page length is"+newpage.getBytes().length);
		ra_f.write(newpage.getBytes());
		ra_f.close();
		}catch(Exception e){
			System.out.println("exception in write to file\n");
		}
	}

	public void newwritetofile(File f,String newpage,String table,int pageno)
	{
		try{
		RandomAccessFile ra_f = new RandomAccessFile(f, "rw");
		long pos=(pageno)*diskblocksize+1;
		//System.out.println("\nwrite pos is "+pos);
		ra_f.seek(pos);
		if(diskblocksize-newpage.getBytes().length!=0){
			//padding
			StringBuffer str_bfr = new StringBuffer();
		for(int i=0;i<(diskblocksize-newpage.getBytes().length);i++)
		{
			//append string to stringbuffer n times
			str_bfr.append(" ");
		}
		//converting stringbuffer back to string using toString() method
		String str = str_bfr.toString();
			newpage+=str;
		}
		//System.out.println(" new page length is"+newpage.getBytes().length);
		ra_f.write(newpage.getBytes());
		ra_f.close();
		}catch(Exception e){
			System.out.println("exception in write to file\n");
		}
	}


	public void seqWrite(String tid,String table,Record rec){
		try
		{
			Logging.logOperation("I "+table+" ("+rec.user_id+", "+rec.user_age+", "+rec.satisfaction_score+")\n");
			File f=new File(table+".txt");
			if(!f.exists())
			{
				//create new file for that table 
				f.createNewFile();
				gnode g=new gnode();
				g.min=5;
				g.max=0;
				g.avg=0;
				g.count=0;
				gOpMap.put(table,g);
			}
			else{
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
			if(!isamMapForEachTable.containsKey(table))
			{
				//if this table X doesn't have any ISAM index list
				isamMapForEachTable.put(table,new ISAMlist());
				//isamMapForEachTable.get(table).addlast(rec.user_id,0,rec.user_id);
			}

			int cacheindex=-1;
			HashMap<Integer,PageTableRecord> records=pagetable.get(table);
			if (records == null) { //the table has no pages.new table. create page for it,swap into cache.
				records = new HashMap<Integer,PageTableRecord>();
				pagetable.put(table, records);
			}
			//get the index in isamlist, where we need to insert.
			ISAMlist indexlist=isamMapForEachTable.get(table);
			int indexonisamlist=indexlist.get(rec.user_id);
						//System.out.println("\nindexonisamlist: "+indexonisamlist);
				
			int pageno;
			 if(indexonisamlist==indexlist.accesslist.size())
			 {//create new page for the record i.e no previous user_id's were less than this user_id
                indexlist.accesslist.add(new ISAMnode(rec.user_id,records.size(),Integer.MAX_VALUE));
				pageno=records.size();
				Logging.logCreateSeq(table,pageno);
			 }
			 else
			 {//found on isam index list. Get the page/block number.then put this page into cache
				pageno =indexlist.accesslist.get(indexonisamlist).blockno;
			 }
			 						//System.out.println("\npage no is:"+pageno);


			String tablerec=rec.user_id+","+rec.user_age+","+rec.satisfaction_score+";\n";
			cacheindex=swapIn(f,table,pageno);//swapin will check if cache is free else applies LRU.
			//swapin returned the index of this page in the cache.
			if(pagetable.get(table).get(pageno)!=null){
			pagetable.get(table).get(pageno).cacheindex=cacheindex;//update the pagetable with new index
			}else{
				PageTableRecord ptrec;
				ptrec=new PageTableRecord(table, true,cacheindex);//table,isdirty,isfull,incache,cacheindex
				records.put(pageno,ptrec);
				pagetable.put(table,records);
			}
			//todo: remove the isfullattribute from PageTableRecord
			String oldpage=cache.get(cacheindex).data; // the data in the diskblock before inserting
			StringTokenizer st1 = new StringTokenizer(oldpage,";\n");  
			int recordsInOldPage=st1.countTokens(); // number of rows already inserted in the diskblock
			String newpage="";
			int records_in_a_block=(int)diskblocksize/12; //max number of records in a block
			int tablerecadded=0;
			int fromuserid=-1;
			int touserid=Integer.MAX_VALUE;
			String carryforward="";//the input for the recursive call.
			//for each record in the block, check if it's user_id is less than the insert.user_id(record to insert)
			while(st1.hasMoreTokens() && (records_in_a_block!=0) ){
				 carryforward=st1.nextToken();
				StringTokenizer st2 = new StringTokenizer(carryforward,",;\n"); //to get the userid
				records_in_a_block--;
				if(Integer.parseInt(st2.nextToken())<rec.user_id){
					newpage+=carryforward+";\n";
				}else{
					if(tablerecadded==0)
						{
							newpage+=tablerec;//inserted the new record.
							tablerecadded=1;
							if(records_in_a_block!=0){
								newpage+=carryforward+";\n";
								records_in_a_block--;
							}
						}
					else{
						newpage+=carryforward+";\n";
					}
				}
			}
			if(newpage.trim().isEmpty()){//if the diskblock/old page was empty, then insert
				newpage+=tablerec;
				tablerecadded=1;
				records_in_a_block--;
			}
			else if(tablerecadded==0 && records_in_a_block!=0){//if there is space in the diskblock,insert to its end
				newpage+=tablerec;
				records_in_a_block--;
				tablerecadded=1;

			}else if(tablerecadded==0 && records_in_a_block==0){ //to do: not sure about this case??????.
				newpage=""+tablerec;
				records_in_a_block=1;
				tablerecadded=1;

			}
			CacheNode cn=new CacheNode(table,pageno,newpage);
			//System.out.println("\nnewpage in cn is  :"+cn.data);
			cache.put(cacheindex,cn); // update the cache page with the inserted record
			newwritetofile(f,newpage,table,pageno);//update the table.txt. we are using write through cache for now
			
			// now lets get the min and max user_id in this new disk block.
			StringTokenizer st = new StringTokenizer(newpage,";\n");    
			// if(st.countTokens()==(int)diskblocksize/12){
			// //if(newpage.trim().getBytes().length>=(diskblocksize-4)){
			// 	records.get(pageno).isfull=true;
			// }
			String prec="";
			while(st.hasMoreTokens()){
				prec=st.nextToken();
				if(fromuserid==-1){
					StringTokenizer st2 = new StringTokenizer(prec,",;\n");
					fromuserid=Integer.parseInt(st2.nextToken());
				}
			}
			StringTokenizer ss = new StringTokenizer(newpage,";\n");    
			if(ss.countTokens()==(int)diskblocksize/12){
				//System.out.println("\ncounttokens is " +ss.countTokens());
			StringTokenizer st2 = new StringTokenizer(prec,";,\n");
			touserid=Integer.parseInt(st2.nextToken());//diskblock is now full. setting the lastuser_id in the block
			}
			//System.out.println("\nfrom and to used id"+fromuserid+"  "+touserid);
			Logging.logWrite(tid,table,rec.user_id,rec.user_age,rec.satisfaction_score);
			//pagetable.remove(table);
			pagetable.put(table,records);

			//update the access listwith the new disk block.fromuserid = first user_id in the disk block
			indexlist.accesslist.set(indexonisamlist,new ISAMnode(fromuserid,pageno,touserid));


			//if one record left after insertion. do recusion add it to next suitable block.
			if((((int)diskblocksize/12)==recordsInOldPage)||tablerecadded==0){
				String lastrec;
				if(tablerecadded==0){
					lastrec=tablerec;
				}
				else{
				 lastrec=carryforward;
				}
				StringTokenizer st3=new StringTokenizer(lastrec,",;\n");
				byte b=Byte.parseByte("0");
				int a=0;
				int u=0;
					String s=st3.nextToken().trim();
					u=Integer.parseInt(s);
						s=st3.nextToken().trim();
						a=Integer.parseInt(s);
						s=st3.nextToken();
						b=Byte.parseByte(s);
						//System.out.println("----------recursive call");
				seqWrite(tid,table,new Record(u,a,b));
			}

		}catch(Exception e){
			System.out.println("cannot open file\n"+e);
		}
	}

 

	
	// public static void main(String args[])
	// {
	// 	Record r=new Record(1,10,(byte)1);
	// 	SeqFileStrategy sf=new SeqFileStrategy(24,48);//disk size, cache size. cache size is a multiple of disk size
	// 	sf.seqWrite("1","X",r);
	// 	sf.seqRead("X",1);
	// 	// r=new Record(2,10,(byte)1);
	// 	// sf.seqWrite("1","X",r);
	// 	r=new Record(3,10,(byte)2);
	// 	sf.seqWrite("1","X",r);
	// 	r=new Record(4,10,(byte)1);
	// 	sf.seqWrite("1","X",r);
	// 			r=new Record(2,20,(byte)2);
	// 	sf.seqWrite("1","X",r);
	// 					r=new Record(5,10,(byte)1);
	// 	sf.seqWrite("1","X",r);
	// 			r=new Record(6,20,(byte)2);
	// 	sf.seqWrite("1","X",r);
	// 	sf.seqRead("X",2);
	// 	sf.seqMRead("X",true,10,Byte.parseByte("0") );//return users whose age is 10

	// 	sf.seqMRead("X",false,0,Byte.parseByte("2") );//return users who gave score as 2
	// 	sf.seqUpdate("1","X",4,Byte.parseByte("3"));


	// }
}