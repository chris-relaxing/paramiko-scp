# paramiko-scp

This Python script automates the running of Hadoop MapReduce jobs, allowing you to run SSH MapReduce jobs and get the outputs without using any cmd windows, PuTTY, or PSCP. It is named after the paramiko and scp modules needed to make this work.

Without this script, running a MapReduce job requires multiple cmd windows and several manual steps. Such as:
1. Carefully build the MapReduce command via a text editor
2. SSH into the HDFS environment using PuTTY to run the job
3. Once the job is complete, run getmerge commands to compile the output into a text document
4. In a separate cmd window, use PSCP to download the output 

The paramiko-scp script eliminates all of these steps. It: <br>

* builds the MR command from a couple of simple inputs, 
* automatically increments a job counter, 
* streams the MR output to your screen in real time, 
* runs the getmerge command, and 
* automatically downloads the file to your PC.


