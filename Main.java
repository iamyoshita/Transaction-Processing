import java.io.*;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws IOException {
       // String scriptDirectory = "scripts"; // this directory has all the test scripts
       System.out.println("Enter testcase directory:");
        Scanner myObj = new Scanner(System.in);
        String scriptDirectory = myObj.nextLine();  
        Logging.filename="outputs/"+scriptDirectory.replaceAll("/", "_")+".txt";
        System.out.println("Choose readMode roundrobin or random: Enter rr or r");
        String readMode = myObj.nextLine();  
        if((!readMode.equals("rr")) && (!readMode.equals("r"))){
            System.out.println("Selected Reading mode is not available in this pittCAP database system.");
            System.exit(1);
        }
        System.out.println("Choose a memory strategy to implement - Sequential File Strategy 'sfs' or LSM Strategy 'lsm':");
        String strategy =myObj.nextLine();  // memory strategy to implement
        System.out.println("Enter cacheCapacity in bytes (should be a multiple of diskblocksize):");
        int cacheCapacity = myObj.nextInt();
        System.out.println("Enter Diskblocksize in bytes (should be a multiple of 12):");
        int diskblocksize = myObj.nextInt();

        // Calling the Transaction Manager
        TransactionManager transactionManager;
        if(strategy.equals("sfs")){
            SeqFileStrategy sfs = new SeqFileStrategy(diskblocksize, cacheCapacity);
            transactionManager = new TransactionManager(scriptDirectory, sfs);
            if(readMode.equals("rr")){
                //System.out.println("**");
                transactionManager.readTransactions(readMode, 0, 0);
            }else {
                System.out.println("Enter randomSeed:");
                long randomSeed = myObj.nextLong();  // seed of the random number generator
                System.out.println("Enter maxLines to be read in one random run:");
                int maxLines = myObj.nextInt();  // maxLines to be read in one random run
                transactionManager.readTransactions(readMode, randomSeed, maxLines);
            }
        }

        else if(strategy.equals("lsm")){
            System.out.println("Enter SSTableCapacity (number of diskblocks in a sstable):");
            int SSTableCapacity = myObj.nextInt();  
            System.out.println("Enter number of SSTables in Level 0:");
            int tablesInLevel0 = myObj.nextInt(); 
            LSMStrategy lsms = new LSMStrategy(SSTableCapacity, cacheCapacity, tablesInLevel0, diskblocksize);
            transactionManager = new TransactionManager(scriptDirectory, lsms);
            if(readMode.equals("rr")){
                transactionManager.readTransactions(readMode, 0, 0);
            }else {
                System.out.println("Enter randomSeed:");
                long randomSeed = myObj.nextLong();  // seed of the random number generator
                System.out.println("Enter maxLines to be read in one random run:");
                int maxLines = myObj.nextInt();  // maxLines to be read in one random run
                transactionManager.readTransactions(readMode, randomSeed, maxLines);
            }
        }                
    }
}