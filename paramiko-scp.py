#-------------------------------------------------------------------------------
# Name:         paramiko scp
# Purpose:      Run SSH MapReduce jobs and get the outputs without using any cmd windows, PuTTY, or PSCP.
#               Named after the paramiko and scp modules needed to make this work.
# Author:       Chris Nielsen <chris.relaxing@gmail.com>
# Created:      01/14/2016
# Notes:        Python 2.7.5; Requires paramiko and scp modules
#-------------------------------------------------------------------------------

import paramiko
from scp import SCPClient
import time
import datetime

# HDFS connection info
host_ip = 'data.in.domain.com'
user = 'data_science113'
pw = 'accessPW'
port = 99

# ----------------------------------------------------
def getDate():
    """Returns a date in the format mmddyy"""
    now = datetime.datetime.now()
    m = now.month
    d = now.day
    y = str(now.year)
    if m < 10:
        m = "0"+str(m)
    if d < 10:
        d = "0"+str(d)
    y = y[2:]
    formatted_date = str(m)+str(d)+str(y)
    return formatted_date

# ----------------------------------------------------
def timestamp():
    """Returns a timestamp in the format hhmmss"""
    now = datetime.datetime.now()
    h = now.hour
    m = now.minute
    s = now.second
    if h < 10:
        h = "0"+str(h)
    if m < 10:
        m = "0"+str(m)
    if s < 10:
        s = "0"+str(s)
    timestamp = str(h)+str(m)+str(s)
    return timestamp # hhmmss
# ----------------------------------------------------

def read_and_increment_counter():
    """Automatically updates the job counter whenever a new MapReduce job is initiated."""
    with open(r'C:\Users\christon\Desktop\MapReduce\job_count.txt', 'r') as f_read:
        count = f_read.readline()
    count = int(count.rstrip())+1
    count = str(count)
    with open(r'C:\Users\christon\Desktop\MapReduce\job_count.txt', 'w') as f_write:
        f_write.write(count+'\n')
    return count

# ----------------------------------------------------

# MapReduce Inputs
product_name = 'TQS_0003'

# Query an individual data ID, or a list of IDs, depending on the product
if product_name == 'New_0015':
    queryId = '688jx7ps-7a550225ee390a315917ddf15e12a316'
    # Limit 1000 Ids at a time
    ##  queryId = ['643ucfs2-9423eda307e94813b03128b2ed8f2805', '643ucfsy-fded8ffe661048878d0ad148b2df37ae', '643ucfsy-6df371d343b14954bdc4bdff31c25dc0', '643ucfsz-fa9fff4693e448dfbe634f4e2185b3c4', '643ucft2-b3b533254bac4305a9b782a6b4c11cc0']
    if type(queryId) is list:
        queryId = ",".join(queryId)
else:
    queryId = '688jx7ps-7a550225ee390a315917ddf15e12a316'


# Housekeeping
date = getDate()
timestamp = timestamp()
job_count = read_and_increment_counter()

output_ref = job_count
print "MapReduce Job:", job_count


# Note to be appended to the name of the MR output
notes = 'New_XML_stats'
if notes:
    notes = '_'+notes
increment = product_name+notes+'_'+timestamp


# Path to HDFS input
input_loc = '/hdfs/new_data/june2017/0000021-160423031304371-oozie-oozi-W/extractOut/*/*.xml'

# Set up other default system paths for the MR command
output_loc = '/user/xml_data/output/cnielsen/'
python_path = "/usr/lib/python_2.7.3/bin/python"
hdfs_home = '/nfs_home/data_science/cnielsen/'
local_dir = r'C:\Users\cnielsen\Desktop\MapReduce\logs'
output_log = r'C:\Users\cnielsen\Desktop\MapReduce\logs\Log'+date+'_'+increment+'.txt'

# File names
xml_lookup_file = 'product_vals.xml'
mapper = 'mapper.py'
reducer = 'reducer.py'
helper_script = 'PlacesValidations.py'
target_file = 'MRout_'+date+'_'+increment+'.txt'

# ----------------------------------------------------

def createSSHClient(host_ip, port, user, pw):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host_ip, port, user, pw)
    return client

# ----------------------------------------------------

def buildMRcommand(product_name, queryId):
    """Build the MapReduce command."""
    space = " "
    mr_command_list = [ 'hadoop', 'jar', '/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar',
                        '-files', hdfs_home+xml_lookup_file,
                        '-file', hdfs_home+mapper,
                        '-file', hdfs_home+reducer,
                        '-mapper', "'"+python_path, mapper, product_name, queryId+"'",
                        '-file', hdfs_home+helper_script,
                        '-reducer', "'"+python_path, reducer+"'",
                        '-input', input_loc,
                        '-output', output_loc+output_ref]

    MR_command = space.join(mr_command_list)
    print MR_command
    write_to_log(MR_command)
    return MR_command

# ----------------------------------------------------

def unbuffered_lines(f):
    line_buf = ""
    while not f.channel.exit_status_ready():
        line_buf += f.read(1)
        if line_buf.endswith('\n'):
            yield line_buf
            line_buf = ""

# ----------------------------------------------------

def write_to_log(text):
    writer = open(output_log, 'a')
    writer.write(text+'\n')
    writer.close()

# ----------------------------------------------------

def stream_output(stdin, stdout, stderr):
    """Steam MR output to the screen while the job is running."""
    writer = open(output_log, 'a')
    # Using line_buffer function
    for l in unbuffered_lines(stderr):
        e = '[stderr] ' + l
        print '[stderr] ' + l.strip('\n')
        writer.write(e)

    # gives full listing..
    for line in stdout:
        r = '[stdout]' + line
        print '[stdout]' + line.strip('\n')
        writer.write(r)
    writer.close()

# ----------------------------------------------------

def run_MapReduce(ssh):
    stdin, stdout, stderr = ssh.exec_command(buildMRcommand(product_name, queryId))
    stream_output(stdin, stdout, stderr)
    return 1

# ----------------------------------------------------

def run_list_dir(ssh):
    list_dir = "ls "+hdfs_home+" -l"
    stdin, stdout, stderr = ssh.exec_command(list_dir)
    stream_output(stdin, stdout, stderr)

# ----------------------------------------------------

def run_getmerge(ssh):
    """Run the getmerge step so it doesn't have to be done manually in a cmd window."""
    getmerge = "hadoop fs -getmerge "+output_loc+output_ref+" "+hdfs_home+target_file
    print getmerge
    stdin, stdout, stderr = ssh.exec_command(getmerge)
    for line in stdout:
        print '[stdout]' + line.strip('\n')
    time.sleep(1.5)
    return 1

# ----------------------------------------------------

def scp_download(ssh):
    """Automatically download the MR output instead of using PSCP."""
    scp = SCPClient(ssh.get_transport())
    print "Fetching SCP data.."
    write_to_log("Fetching SCP data..")
    scp.get(hdfs_home+target_file, local_dir)
    print "File download complete."
    write_to_log("File download complete.")

# ----------------------------------------------------
def startTimer():
    starttime = time.time()
    return starttime
# ----------------------------------------------------

def endTimer(starttime):
    endtime = time.time()
    hours = int((endtime - starttime)/3600)
    minutes = int(((endtime - starttime) - (hours *3600))/60)
    seconds = (endtime - starttime) - (hours *3600) - (minutes * 60)
    retstr = ''
    if hours > 1:
            retstr += '%d hours ' % hours
    elif hours == 1:
            retstr += '1 hour '
    if minutes > 1:
            retstr += '%d minutes ' % minutes
    elif minutes == 1:
            retstr += '1 minute '
    if seconds > 1:
            retstr += '%d seconds' % seconds
    elif hours == 1:
            retstr += '1 second'
    return retstr

# ----------------------------------------------------

def main():

    runtime = startTimer()

    # Get the ssh connection
    global ssh
    ssh = createSSHClient(host_ip, port, user, pw)
    write_to_log("MapReduce Job: "+job_count)
    print "Executing command..."
    write_to_log("Executing command...")

    # Command list
    ##    run_list_dir(ssh)
    ##    run_getmerge(ssh)
    ##    scp_download(ssh)

    # Run MapReduce
    MR_status = 0
    MR_status = run_MapReduce(ssh)

    if MR_status == 1:
        gs = 0
        gs = run_getmerge(ssh)
        if gs == 1:
            scp_download(ssh)

    # Close ssh connection
    ssh.close()
    print 'Execution time: ',endTimer(runtime)
    et = 'Execution time: '+endTimer(runtime)
    write_to_log(et)


if __name__ == '__main__':
    main()

