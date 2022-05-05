import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
public class Logging{
    static File logfile;
    static String filename;//="log.txt";
    public Logging(){
        //this.filename="log.txt";
        if(!logfile.exists()){
        logfile=new File(filename);
        }
    }

    public static void logOperation(String line){
        try{
        FileWriter fw=new FileWriter(filename,true);
        fw.write(line+"\n");
        fw.close();
        }catch(IOException err){
            System.out.println("couldn't log\n");
        }
    }

    public static void logAbort(String tid){
        logOperation("A "+tid+"\n");

    }
    public static void logCommit(String tid){
        logOperation("C "+tid+"\n");
        logOperation("-----------------------\n");

    }
    public static void logBegin(String tid){
        // if(txn){
        // logOperation("BEGIN txn "+tid+"\n");
        // }else{
            logOperation("BEGIN "+tid+"\n");
        // }
    }
    public static void logRead(String table,int userid,int userage,byte score){
        logOperation("READ: "+table+","+userid+","+userage+","+score+"\n");
    }
    public static void logCreateSeq(String table,int pageno){
        logOperation("CREATE T-"+table+" P-"+pageno+"\n");
    }
    public static void logCreateLSM(String tid,int levelno,String table1,int key1, String table2,int key2){
        logOperation("CREATE "+table1+" L-"+levelno+" K-"+table1+key1+"-"+table2+key2+"\n");
    }
    public static void logSwapSeq(boolean in,String table,int pageno){
        if(in){
        logOperation("SWAP IN T-"+table+" P-"+pageno+"\n");
        }
        else{
            logOperation("SWAP OUT T-"+table+" P-"+pageno+"\n");
        }
    }
    public static void logSwapLSM(boolean in,int levelno,String table1,int key1, String table2,int key2){
        if(in){
            logOperation("SWAP IN L-"+levelno+" K-"+table1+key1+"-"+table2+key2+"\n");
        }else{
            logOperation("SWAP OUT L-"+levelno+" K-"+table1+key1+"-"+table2+key2+"\n");
        }
    }
    public static void logWrite(String tid,String table,int userid,int userage,byte score){
        logOperation(tid+" WRITTEN: "+table+","+userid+","+userage+","+score+"\n");
    }
    public static void logMRead(String table,int userid,int userage,byte score){
        logOperation("MREAD: "+table+","+userid+","+userage+","+score+"\n");
    }
    public static void logUpdate(String tid, String table,int userid,int age,byte score,byte prev){
        logOperation(tid+ " UPDATED: "+table+","+userid+","+age+","+score+" prev: "+prev+"\n");
    }
    public static void logG(String table,String op,double g){
        logOperation("G: "+table+","+op+": "+g+"\n");
    }
}