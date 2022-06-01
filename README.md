-bash-4.1$ rm -rf X.txt log.txt

-bash-4.1$ javac *.java

-bash-4.1$ java Main

Check REPORT.pdf for additional details


Scheduler:
Consists of :
•	RecoveryManager.java
•	Scheduler.java
•	LockTable.java
•	LockManager.java

Recovery manager has abort function that reads the log file backwards and reverts the write and update operations for LSM. We could not implement recovery for Sequential disk due to time limitations.

LockTable.java has the code for acquirelock,releaselock and detecting deadlock using the provided py file.

The locks we used are T-WL,T-RL,P-WL,T-MREAD.

P is for process and it has only one lock i.e P-WL. This is because processes are read uncommitted, hence they don’t need read locks. And a process can read any data even if it is locked. 



Executing this project:

We are using python file provided for detecting deadlocks. Hence the location of python must be specified in the project. Please make the following change to run the project locally:
In LockTable.java, line 224:
            ProcessBuilder processBuilder = new ProcessBuilder("C:/Users/Yoshita/AppData/Local/Programs/Python/Python39/python.exe", "C:/Users/Yoshita/Desktop/CS2550-DBMS-project-1/DeadlockDetector.py");

Update the location of python.exe. and the location of the DeadlockDetector.py code which we have provided in the repository.

Delete any tables from previous execution like X.txt files as the code just appends to existing files.

To execute the project we can run the following commands:

-bash-4.1$ javac *.java

-bash-4.1$ java Main

Now enter the required input like disk block size etc. Once execution finishes, the output will be stored in outputs folder.

Sample arguments: in this case the output log will be stored in outputs/test_consurrency_normal_Test1.txt

Enter testcase directory:
tests/concurrency/normal/Test1
Choose readMode roundrobin or random: Enter rr or r
rr
Choose a memory strategy to implement - Sequential File Strategy 'sfs' or LSM Strategy 'lsm':
lsm
Enter cacheCapacity in bytes (should be a multiple of diskblocksize):
48
Enter Diskblocksize in bytes (should be a multiple of 12):
24
Enter SSTableCapacity (number of diskblocks in a sstable):
2

