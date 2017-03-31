#!/usr/bin/python
import os, sys, commands
import time
"""
Some function for interating with shell
os.system('cat /proc/cpuinfo')
output = os.popen('cat /proc/cpuinfo')
print output.read()
(status, output) = commands.getstatusoutput('cat /proc/cpuinfo')
commands.getoutput('ls /bin/ls')
commands.getstatus('/bin/ls')
"""

Usage="""
    Usage: $0 number_core sge_list [exclude_nodes_list]
    
    Name in sge_list should be ab_path
    The qsub script equal to:
    qsub -cwd -j y -S /bin/bash -q node_name -pe cluster_name number_core each_job.sge

    [-a date_time] [-c interval] [-C directive_prefix]
    [-e path] [-I] [-j join] [-k keep] [-l resource_list] [-m mail_options]
    [-M user_list][-N name] [-o path] [-p priority] [-q destination] [-r c]
    [-S path_list] [-u user_list][-v variable_list] [-V]
    [-W additional_attributes] [-z]

    Default:
    -C #$   :start with #$ means it is a qsub cimmand.
    -cwd    :run in current working directory
    -o      :cwd/script.log
    -j yes  :add err to log
    -S /bin/bash
    """
if len(sys.argv) < 2:
    print(Usage)
    sys.exit()

pe = int(sys.argv[1])
sge_list = open(sys.argv[2],'r').readlines()
logfile = open(sys.argv[2]+ ".log",'w')
myhost = os.uname()[1][0]
cwd = os.getcwd()

if len(sys.argv) == 4: ex_node_abspath=os.path.abspath(sys.argv[3])

def get_cores():
    exclude_nodes_list = []
    if len(sys.argv) == 4:
        with open(ex_node_abspath,'r') as ex:
            exclude_nodes_list = ex.readlines()
    cores = [] 
    empty_nodes = []
    no_empty_nodes = []
    if myhost in 'kn':
        print(" You are in %s cluster."%(myhost))
        #nodes= commands.getoutput("qstat -f|grep @%s"%(myhost)).split("\n")
        nodes= commands.getoutput("qstat -f|grep @").split("\n")
    for line in nodes:
        if len(line.split()) == 6:continue #this node have error
        node = line.split()[0]
        if node + '\n' in exclude_nodes_list:
            print("%s in exclude_nodes_list\n"%(node))
            continue
        cluster_name = node.split('@')[0]
        avail = int( 8 - int(line[39]))
        # print(avail)
        if avail < 8 :
            no_empty_nodes.append((cluster_name,node,avail))
        else:
            empty_nodes.append((cluster_name,node,avail))
    for (cluster_name,node,avail) in ( no_empty_nodes + empty_nodes ):
        # print((cluster_name,node,avail))
        for core_count in range(avail):
            cores.append((cluster_name, node, avail))
            avail -= 1
    return cores

cores = get_cores()
core_index = 0
core_num = len(cores) - 1
qsubcmd="qsub  -j y -S /bin/bash  -pe %s %d -q %s  %s"
#qsubcmd="qsub -cwd -j y -S /bin/bash  -pe %s %d -q %s -o %s %s"

if len(sge_list) * pe > core_num:
    logfile.write ("\n!!! Empty clusters are not enough.\n!!! Some jobs will wait.\n")

sge_num = len(sge_list)
sge_qsub_num = 0
for sge in sge_list:
    sge = sge.rstrip()

    #No enough core in this nodes, goto next node.
    while (cores[core_index][2] < pe and core_index + pe <= core_num):
        core_index += cores[core_index][2]

    # if cores list is end, get list again
    while core_index + pe > core_num:
        # get empty cores repeatly per 5 mins until cores >= pe!!!!
        time.sleep(300) # wait 5 mins
        core_index = 0
        cores = get_cores()
        core_num  = len(cores) - 1
        #No enough core in this nodes, goto next node.
        while (core_num >= 0 and cores[core_index][2] < pe and core_index + pe <= core_num):
             core_index += cores[core_index][2]

    cluster_name,node,avail = cores[core_index]
    path = '/'.join(sge.split('/')[:-1])
    log = sge + '.log'
    #print(path)
    os.chdir(path)
    #print(qsubcmd%(cluster_name, pe, node, log, sge))
    
    #qsub job 
    if '@k' in node:
        os.system("scp %s k:tmp/tmp.sge && ssh -t k qsub  -j y -S /bin/bash  -pe %s %d -q %s  tmp/tmp.sge"%(
                    sge, cluster_name, pe, node))
    if '@n' in node:
        os.system("scp %s n:tmp/tmp.sge && ssh -t n qsub  -j y -S /bin/bash  -pe %s %d -q %s  tmp/tmp.sge"%(
                    sge, cluster_name, pe, node))
        
    #os.system(qsubcmd%(cluster_name, pe, node, log, sge))
    sge_qsub_num += 1
    logfile.write("Had qsub %d/%d jobs\n"%(sge_qsub_num, sge_num))
    logfile.write("Last qsub sge is :%s\n"%(sge))
    logfile.flush()
    core_index += pe 

logfile.close()
mail_attach=cwd + "/"+sys.argv[2]+ ".log"
mail_body="Had qsub %d/%d jobs\n"%(sge_qsub_num, sge_num)
os.system("sm.py  807132354@qq.com qsubJob_done %s %s"%(mail_attach,mail_body))
