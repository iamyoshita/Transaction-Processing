import java.util.*;
import java.io.*;
public class RecoveryManager {
    public HashMap<String,Integer> transactionBegin=new HashMap<String,Integer>();
    void begin(String tid){
        //store the begin line number of the transaction
        BufferedReader reader;
        int linecount=0;
		try {
			reader = new BufferedReader(new FileReader(Logging.filename));
			String line = reader.readLine();
			while (line != null) {
				// System.out.println(line);
				line = reader.readLine();
                linecount++;
			}
			reader.close();
            System.out.println("line count in rm:begin"+linecount);
            transactionBegin.put(tid,linecount);
		} catch (IOException e) {
			e.printStackTrace();
		}

    }
    void abort(String tid){

        //to undo insert and update
        BufferedReader reader;
        int linecount=transactionBegin.get(tid);
		try {
            ArrayList<String> lines = new ArrayList<String>();
			reader = new BufferedReader(new FileReader(Logging.filename));
			String line=null;// = reader.readLine();
            //goto the begin of txnid in the log
			while (linecount != 0) {
				// System.out.println(line);
				line = reader.readLine();
                linecount--;
			}
            //store all lines with the txnid
            while(line!=null){
                //System.out.println("in abort func in RM line read is "+line);
                StringTokenizer st = new StringTokenizer(line," ");
                if(st.hasMoreTokens()){
				String txid=st.nextToken();
                if(txid.equals(tid)){
                    lines.add(line);
                }}
                line=reader.readLine();
            }
			reader.close();

            //start reading the txn ops from back
            for(int i=lines.size()-1;i>=0;i--){
                line=lines.get(i);
                String op=line.split(" ")[1];
                if(op.equals("WRITTEN:")){
                    //tid+" WRITTEN: "+table+","+userid+","+userage+","+score+"\n");
                    Record rec=new Record(Integer.parseInt(line.split("[ ,]")[3]),-1,Byte.parseByte("0"));
                    TransactionManager.lsm.writeLSM(tid,line.split("[ ,]")[2],rec);
                    System.out.println("reverted write\n");

                }else if(op.equals("UPDATED:")){
                    //(tid+ " UPDATED: "+table+","+userid+","+age+","+score+" prev: "+prev+"\n");
                    TransactionManager.lsm.updateLSM(tid,line.split("[ ,]")[2],Integer.parseInt(line.split("[ ,]")[3]),Byte.parseByte(line.split("[ ,]")[7]));
                    System.out.println("reverted update\n");
                }
            }
		} catch (IOException e) {
			e.printStackTrace();
		}

    }
    void commit(String tid){

    }
    
}
