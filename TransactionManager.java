import java.io.*;
import java.util.*;
//import java.text.DecimalFormat;

public class TransactionManager {

    static String testscripts;
    //static Scheduler scheduler;
    static SeqFileStrategy sfs=null;
    static LSMStrategy lsm=null;
    static Scheduler sch=null;
    long startTime;

    // constructor for Sequential File Strategy
    public TransactionManager(String testscripts, SeqFileStrategy sfs) {
        this.testscripts = testscripts;
        this.sfs = sfs;
        sch = new Scheduler(sfs);
    }

    // constructor for LSM Strategy
    public TransactionManager(String testscripts, LSMStrategy lsms) {
        this.testscripts = testscripts;
        this.lsm = lsms;
        sch = new Scheduler(lsm);
    }

    public void readTransactions(String readMode, long randomSeed, int maxLines) throws IOException {
        startTime = System.currentTimeMillis();
        Map transactionBuffer = new HashMap<>();
        Map transactionMode = new HashMap<>();

        File filePath = new File(testscripts);
        List list = new ArrayList();
        List fileList = getFiles(filePath, list);
        //System.out.println(fileList);

        for (Object file : fileList) {
            FileInputStream inputStream = new FileInputStream(file.toString());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
           // String input = bufferedReader.readLine();
           // String EMode = input.substring(input.length() - 1);
           // if (EMode.equals("1")) {  //new transaction (EMode=1)
              transactionMode.put(file, 1);
                transactionBuffer.put(file, bufferedReader);
            // } else if (EMode.equals("0")) {  //new process (EMode=0)
            //     transactionMode.put(file, 0);
            //     transactionBuffer.put(file, bufferedReader);
            // }
        }
        if (readMode.equals("rr")) {
            roundRobin(fileList, transactionBuffer, transactionMode);
        } else {
            random(fileList, transactionBuffer, transactionMode, randomSeed, maxLines);
        }

        System.out.println("Total commands "+sch.totalcommands);
        System.out.println("Total commits "+sch.totalcommits);
        System.out.println("Total reads "+sch.totalreads);
        System.out.println("Total writes "+sch.totalwrites);
        if(sch.totalreads!=0)
        System.out.println("Average read time in milliseconds "+sch.readtime/sch.totalreads);
        if(sch.totalwrites!=0)
        System.out.println("Average write time in milliseconds "+sch.writetime/sch.totalwrites);
    }

    private List getFiles(File filePath, List list) {
        File[] fs = filePath.listFiles();
        for(File f:fs){
            if(f.isDirectory())	
                getFiles(f, list);
            if(f.isFile())	
                list.add(f);
        }
        return list;
    }


    private void roundRobin(List fileList, Map transactionBuffer, Map transactionMode) throws IOException {
        //System.out.println("Round Robin Reading: " + testscripts);
        List transactionList = new ArrayList();
        List processList = new ArrayList();

        while (!transactionMode.isEmpty()) {
            for (int i = 0; i < fileList.size(); i++) {
                Object file = fileList.get(i);
                BufferedReader buffer = (BufferedReader) transactionBuffer.get(file);
                if (buffer == null) continue;
                String input = buffer.readLine();
                if (input == null) {
                    fileList.remove(file);
                    transactionBuffer.remove(file);
                    transactionMode.remove(file);
                    continue;
                }
                int nameBegin = file.toString().lastIndexOf("\\") + 1;

                if(input.split(" ")[0].equals("G")){
                    String op = input.split(" ")[2];
                    String table_name=input.split(" ")[1];
                    //op=op.substring(0,op.length()-1); 
                    int aggregation_op = 2;
                    if(op.equals("MIN")){
                        aggregation_op = 0;
                    } else if(op.equals("MAX")){
                        aggregation_op = 1;
                    } else{
                        aggregation_op = 2;
                    }
                    if(sfs != null){
                        sfs.seqG(table_name, aggregation_op);
                    }else{
                        lsm.GLSM(table_name, aggregation_op);
                    }
                }else{
                   // System.out.print(input+"in TM\n");
                   String s = file.toString().substring(nameBegin);
                //     s = s.substring(s.lastIndexOf("\\")+1);

                sch.execute(input,file.toString().substring(nameBegin));
                }
                // if (transactionMode.get(file).toString().equals("0")) {
                //     String s = file.toString().substring(nameBegin);
                //     s = s.substring(s.lastIndexOf("\\")+1);
                //     //System.out.println("s and input "+s+" "+input);
                //     transactionList.add(new ArrayList<>(Arrays.asList(s, input)));
                // } else {
                //     processList.add(new ArrayList<>(Arrays.asList(file.toString().substring(nameBegin), input)));
                // }
            }
        }     
        //processTransactions(transactionList);
    }

    private void random(List fileList, Map transactionBuffer, Map transactionMode, long randomSeed, int maxLines) throws IOException {
        System.out.println("Random Reading: " + testscripts);
        List transactionList = new ArrayList();
        List processList = new ArrayList();
        Random ran = new Random(randomSeed);
        while (!transactionMode.isEmpty()) {
            int fileIndex = ran.nextInt(transactionMode.size());
            Object fileName = fileList.get(fileIndex);
            BufferedReader buffer = (BufferedReader) transactionBuffer.get(fileName);
            if (buffer == null){
                fileList.remove(fileName);
                continue;
            }
            int nameBegin = fileName.toString().lastIndexOf("/") + 1;
            int readRow = ran.nextInt(maxLines);
            while(readRow > 0){
                String input = buffer.readLine();
                if (input == null) {
                    transactionBuffer.remove(fileName);
                    transactionMode.remove(fileName);
                    fileList.remove(fileName);
                    break;
                }

                if(input.split(" ")[0].equals("G")){
                    String op = input.split(" ")[2];
                    String table_name=input.split(" ")[1];
                    //op=op.substring(0,op.length()-1); 
                    int aggregation_op = 2;
                    if(op.equals("MIN")){
                        aggregation_op = 0;
                    } else if(op.equals("MAX")){
                        aggregation_op = 1;
                    } else{
                        aggregation_op = 2;
                    }
                    if(sfs != null){
                        sfs.seqG(table_name, aggregation_op);
                    }else{
                        lsm.GLSM(table_name, aggregation_op);
                    }
                }else{
                sch.execute(input,fileName.toString().substring(nameBegin));
                }

                // if (transactionMode.get(fileName).toString().equals("0")) {
                //     transactionList.add(new ArrayList<>(Arrays.asList(fileName.toString().substring(nameBegin), input)));
                // } else {
                //     processList.add(new ArrayList<>(Arrays.asList(fileName.toString().substring(nameBegin), input)));
                // }
                readRow--;
            }
        }
        //processTransactions(transactionList);
    }

    private void processTransactions(List transactionList){
        for (Object t: transactionList){
            System.out.println(t.toString());
            String tid=t.toString().split(" ")[0];
            String operation = t.toString().split(" ")[1];
            String table_name="";
            if( t.toString().split(" ").length>2){
            table_name = t.toString().split(" ")[2];  
            }          
                                 
           // System.out.println("operation"+operation);
            if(operation.equals("R")){
                String id = t.toString().split(" ")[3];
                id=id.substring(0,id.length()-1);  
                if(sfs!=null){
                   // System.out.println("called read in sfs");
                sfs.seqRead(table_name, Integer.parseInt(id));
                }else{
                lsm.readLSM(tid,table_name, Integer.parseInt(id));
                }
            }
            else if(operation.equals("M")){
                String option = t.toString().split(" ")[3];
                String score = t.toString().split(" ")[4]; 
                score=score.substring(0,score.length()-1);  
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
                lsm.mreadLSM(tid,table_name, selected_option, age, Byte.parseByte(score));
                }
            }
            else if(operation.equals("U")){
                int id = Integer.parseInt(t.toString().split(" ")[3]);
                String sc = t.toString().split(" ")[4]; 
                sc=sc.substring(0,sc.length()-1);      
                Byte score = Byte.parseByte(sc);
                if(sfs!=null){
                sfs.seqUpdate(tid,table_name, id, score);}
                else{
                lsm.updateLSM(tid,table_name, id, score);
                }
            }
            else if(operation.equals("I")){
                String details = t.toString().substring(8).trim().replaceAll("[(,)]+","");  
                //System.out.println("called insert"+details);
                int user_id = Integer.parseInt(details.split(" ")[0]);
                int user_age = Integer.parseInt(details.split(" ")[1]);
                String sc = details.split(" ")[2]; 
                sc=sc.substring(0,sc.length()-1); 
                byte satisfaction_score = Byte.parseByte(sc);

                Record record = new Record(user_id, user_age, satisfaction_score);
                if(sfs!=null){
                sfs.seqWrite(tid,table_name, record);}
                else{
                lsm.writeLSM(tid,table_name, record);
                }
            }
            else if(operation.equals("G")){
                String op = t.toString().split(" ")[3];
                op=op.substring(0,op.length()-1); 
                int aggregation_op = 2;
                if(op.equals("MIN")){
                    aggregation_op = 0;
                } else if(op.equals("MAX")){
                    aggregation_op = 1;
                } else{
                    aggregation_op = 2;
                }
                if(sfs != null){
                    sfs.seqG(table_name, aggregation_op);
                }else{
                    lsm.GLSM(table_name, aggregation_op);
                }
            }
        }
    }
}