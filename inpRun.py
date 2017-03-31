#!/bin/env python
import os,sys
import argparse
def argParser():
    usage = ("Organize not run .inp files into folders.")

    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument("-j", "--job", choices = ['org','qsub'],
        help="org: organize,check input inps and put them in folders; qsub: write qsubf only") 
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-d","--dirs_list_common",
        help="os.walk('./') and go into dirs with common part of DIRS_LIST")
    group.add_argument("-i", "--inplist", help="List of inp files")
    
    parser.add_argument("-s", "--size",type=int,default=1000, 
        help="How many file in a floder, default=1000")
    
    parser.add_argument('-p',"--prefix",default='org', 
        help="Prefix of folders organized, default='org'")
    
    parser.add_argument("-n", "--name_of_pair", default='xxx_wtr',
        help="Pair name, e.g.: wtr_wtr, default='xxx_wtr'")
    
    parser.add_argument('-c',"--check",choices = ['mp2','ene','no'],default='no',
        help="Go into .inp.log and Check 'E(MP2)=','TOTAL ENERGY =' or not. Slower. default='no'")

    parser.add_argument("--copy", action='store_true',
        help="Copy files. if not, move files.")
    
    return parser

def retry_rsync_cmd(SRC,DEST):
    """
    http://blog.iangreenleaf.com/2009/03/rsync-and-retrying-until-we-get-it.html
    """
    rsync_bash_cmd= ('\ntrap "echo Exited!; exit;" SIGINT SIGTERM\n' +
                     'MAX_RETRIES=50\n' +
                     'i=0\n'+
                     'false\n' +
                     'while [ $? -ne 0 -a $i -lt $MAX_RETRIES ]\n'+
                     'do\n' +
                     '    i=$(($i+1))\n' +
                     '    sleep 3s\n' +
                     '    rsync -aP %s %s\n'%(SRC, DEST) +
                     'done\n' +
                     'if [ $i -eq $MAX_RETRIES ]\n' +
                     'then\n' +
                     '    echo "Hit maximum number of retries, giving up."\n' +
                     'fi\n\n')
    return rsync_bash_cmd

def write_qsubf(qsubf):
    rundimer = '~/.scripts/RunDimer.py'
    home=os.getcwd()
    abpath = os.path.abspath(qsubf)
    dirpath = os.path.dirname(abpath)
    dirname = os.path.basename(dirpath)
    myhost = os.uname()[1][0]
    tmpcwd = '/pubdata/jcyang/QM'

    cmd = ( "#!/bin/bash\n" +
            "#$ -S /bin/bash\n"+
#           "#$ -o /pubdata/jcyang/QM\n" + # log default in ~
            "#$ -N %s_%s\n"%(dirname,args.name_of_pair) + 
            "source ~/.bashrc\n" + 
            "for i in `ipcs -s | sed -n '4,$p' | cut -d" " -f2 ` ;do ipcrm -s $i ;done\n" + # for free RAM for gemess
            "echo ${HOSTNAME}:~/${JOB_NAME}.o${JOB_ID} at `date` >> ~/qsub.log\n" +
            'SRC=%s/gms_"$JOB_ID"\n'%(tmpcwd) +
            "mkdir -p $SRC || ( rm -r %s && mkdir -p $SRC)\n"%(tmpcwd) + # QM may become a file for log(-o) setting.
            "cp %s $SRC\n"%(rundimer) +
            #"scp %s:%s/*.inp $SRC\n"%(myhost, dirpath) +
            retry_rsync_cmd("%s:%s/*.inp"%(myhost, dirpath), '$SRC') +
            "cd $SRC\n"+
            "python RunDimer.py\n" +
            #"scp * %s:%s && rm -rf $SRC\n"%(myhost, dirpath) 
            retry_rsync_cmd("*","%s:%s/"%(myhost, dirpath)) +
            'rm -rf $SRC\n'
            )
    with open(qsubf,"w") as f: f.write(cmd)


def done_job(inpf):
    logf = inpf + '.log'
    if not os.path.exists(logf):return 0
    else:
        if args.check=='no':return 1 #default='no'
        if args.check=='mp2':keyword="E(MP2)="
        if args.check=='ene':keyword="TOTAL ENERGY ="
        temppipe = os.popen('grep "%s" %s'%(keyword,logf))
        templines = temppipe.readlines()
        temppipe.close()
        if len(templines) >= 1:
            return 1
        else:
            return 0

def next_inp():
    if args.inplist:
        with open(args.inplist,"r") as f:
            for inpf in f.readlines():
                inpf = inpf.rstrip()
                if not done_job(inpf): yield inpf
    if args.dirs_list_common:
        for root,dirs,files in os.walk('./'):
            if args.dirs_list_common not in root:continue
            for name in files:
                if name.endswith('.inp'):
                    inpf = os.path.join(root,name)
                    if not done_job(inpf): yield inpf

def get_folder_name(start=0):
    """
    input count
    return count and folder_name
    """
    if start== 0:
        exist_folders = os.popen("ls -d %s_* 2>/dev/null"%(args.prefix),'r').readlines()
        tmpDirList = []
        if len(exist_folders) == 0:start = 0
        else:
            for line in exist_folders:
                line = line.rstrip()
                tmpDirList.append(int(line.split('_')[1]))
            tmpDirList.sort()
            start = tmpDirList[-1] + 1
    folder_name = "%s_%04d"%(args.prefix,start)
    return start, folder_name

def split_inp():
    files_count = 0
    folder_count = 0
    for inp in next_inp():
        if files_count % args.size == 0:
            folder_count, folder_name = get_folder_name(start=folder_count)
            if not os.path.exists(folder_name):os.mkdir(folder_name)
            qsubf = os.path.join(folder_name,'qall.sh')
            write_qsubf(qsubf)
            folder_count += 1
        if args.copy:
            os.system('cp %s %s'%(inp,folder_name))
        else:
            os.system('mv %s %s'%(inp,folder_name))
        files_count += 1

def write_qsub_files():
    folders = os.popen("ls -d %s_* 2>/dev/null"%(args.prefix),'r').readlines()
    for f in folders:
        qsubf = os.path.join(f.rstrip(),'qall.sh')
        write_qsubf(qsubf)


if __name__ == '__main__':
    parser = argParser()
    args = parser.parse_args()
    if len(sys.argv) < 3:
        parser.print_help()
    if args.job == 'org':split_inp()
    if args.job == 'qsub' :write_qsub_files()
