import os
import subprocess

#NOTE: This is meant to transfer two files in the format: p_c_r_scanpoints.txt and p_c_r_*_tarball.tar.xz

# /tmp/x509up_u92084

# gfal-stat 'gsiftp://deepthought.crc.nd.edu/hadoop/store/user/awightma/gridpack_scans/ctG/ttH_ctG_run2_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz'
# gfal-sum 'gsiftp://deepthought.crc.nd.edu/hadoop/store/user/awightma/gridpack_scans/ctG/ttH_ctG_run2_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz' MD5
# scp awightma@lxplus.cern.ch:/afs/cern.ch/work/a/awightma/private/gridpack_production/genproductions/bin/MadGraph5_aMCatNLO awightma@earth.crc.nd.edu:/hadoop/store/user/awightma/gridpack_scans/2018_05_06/


MAX_TRANSFERS = 999  # Limit the number of transfers per code running
def main():
    good_fname = 'good_copies.log'
    bad_fname = 'failed_copies.log'

    #source_dir = "/afs/cern.ch/user/a/awightma/workspace/private/gridpack_production/genproductions/bin/MadGraph5_aMCatNLO"
    #source_host = ""

    protocol = 'gsiftp://'
    tar_host = 'deepthought.crc.nd.edu'
    tar_dir = '/hadoop/store/user/awightma/gridpack_scans/2018_05_06/'
    sub_dir = '/scanpoints/'
    failed_copies = []
    good_copies = []

    indent_lvl = 1
    indent = "\t"*indent_lvl

    process_whitelist = []
    coeff_whitelist   = []
    run_whitelist     = []

    transfer_files = getFilesToTransfer('.',process_whitelist,coeff_whitelist,run_whitelist)
    for idx,fn in enumerate(transfer_files):
        if idx > MAX_TRANSFERS:
            break
        bad_copy = False
        remote_fn = protocol+tar_host+tar_dir+fn
        if "_scanpoints.txt" in fn:
            remote_fn = protocol+tar_host+tar_dir+sub_dir+fn

        print "#"*100
        print "[%d/%d] Transfering File: %s" % (idx+1,len(transfer_files),fn)
        
        local_sz      = getFileSize(fn)
        local_chksum  = getCheckSum(fn)
        remote_sz     = getFileSize(remote_fn)
        remote_chksum = getCheckSum(remote_fn) if remote_sz != -1 else -1

        if remote_chksum == local_chksum:
            # The file is already present and has correct checksum
            print "%sRemote file already exists, skipping..." % (indent)
            print ""
            continue

        print "%sTarget: %s" % (indent,remote_fn)
        print "%sSize: %.2f MB" % (indent,float(local_sz)/(1024*1024))

        try:
            stdout_arr = run_process(['gfal-copy','-fp',fn,remote_fn],indent=1)
            copied_chksum = getCheckSum(remote_fn)
        except KeyboardInterrupt:
            print "Ending early!"
            failed_copies.append(fn)
            break

        if copied_chksum != local_chksum:
            print "%sResult: FAILED" % (indent)
            failed_copies.append(fn)
        else:
            print "%sResult: SUCCESS" % (indent)
            good_copies.append(fn)
        print "%sDone!" % (indent)

    with open(bad_fname,'w') as f:
        f.write('failed copies:\n')
        for fname in failed_copies:
            f.write(fname)
            f.write('\n')

    with open(good_fname,'w') as f:
        f.write('good copies:\n')
        for fname in good_copies:
            f.write(fname)
            f.write('\n')
    return

# Get a list of all (local) files to transfer
def getFilesToTransfer(fdir='.',p_wl=[],c_wl=[],r_wl=[]):
    search_strs = ['_tarball.tar.xz','_scanpoints.txt']
    files = []
    arr = getLocalFiles(fdir)
    #for idx,f in enumerate(os.listdir(fdir)):
    for idx,f in enumerate(arr):
        if not isValidOr(f,search_strs):
            # The file does not contain any of the search strings
            continue
        arr = f.split('_')
        if len(arr) < 3:
            continue
        p,c,r = arr[:3]

        if len(p_wl) > 0 and not p in p_wl:
            continue
        elif len(c_wl) > 0 and not c in c_wl:
            continue
        elif len(r_wl) > 0 and not r in r_wl:
            continue

        cross_checks = [
            "%s_%s_%s_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz" % (p,c,r),
            "%s_%s_%s_scanpoints.txt" % (p,c,r)
        ]
        if not os.path.exists(cross_checks[0]) or not os.path.exists(cross_checks[1]):
            # The file is missing a complement
            continue
        files.append(f)
    return files

# Get a list of local files in a directory
def getLocalFiles(fdir='.'):
    files = []
    for idx,f in enumerate(os.listdir(fdir)):
        if os.path.isdir(f):
            continue
        files.append(f)
    return files

# Get a list of files from a remote directory
def getRemoteFiles(fdir):
    # NOTE: Not currently finished!
    files = []
    arr = run_process(['gfal-ls',fdir],verbose=True)
    print arr

    return files

# fname must satisfy at least one of the search strings
def isValidOr(fname,search_strs):
    for s in search_strs:
        if s in fname:
            return True
    return False

# Returns the target file size using gfal-stat
def getFileSize(f):
    arr = run_process(['gfal-stat',f],verbose=False)
    if len(arr) < 2:
        return -1

    if not 'Size: ' in arr[1]:
        return -1

    return int(arr[1].split('\t')[0][6:])

# Returns the MD5 checksum of target file using gfal-sum
def getCheckSum(f):
    arr = run_process(['gfal-sum',f,'MD5'],verbose=False)
    if len(arr) != 1:
        return -1
    return arr[0].split()[1]

def run_process(inputs,verbose=True,indent=0):
    indent_str = "\t"*indent
    p = subprocess.Popen(inputs,stdout=subprocess.PIPE)
    stdout = []
    while True:
        l = p.stdout.readline()
        if l == '' and p.poll() is not None:
            break
        if l:
            stdout.append(l.strip())
            if verbose: print indent_str+l.strip()
    return stdout


if __name__ == "__main__":
    main()
