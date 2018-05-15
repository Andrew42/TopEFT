import os
import subprocess

#NOTE: This is meant to transfer two files in the format: p_c_r_scanpoints.txt and p_c_r_*_tarball.tar.xz

# /tmp/x509up_u92084

# gfal-stat 'gsiftp://deepthought.crc.nd.edu/hadoop/store/user/awightma/gridpack_scans/ctG/ttH_ctG_run2_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz'
# gfal-sum 'gsiftp://deepthought.crc.nd.edu/hadoop/store/user/awightma/gridpack_scans/ctG/ttH_ctG_run2_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz' MD5

MAX_TRANSFERS = 999  # Limit the number of transfers per code running
def main():
    protocol = 'gsiftp://deepthought.crc.nd.edu'
    outdir = '/hadoop/store/user/awightma/gridpack_scans/2018_05_06/'
    sub_dir = '/scanpoints/'
    failed_copies = []
    good_copies = []

    transfer_files = getFilesToTransfer('.')
    for idx,f in enumerate(transfer_files):
        if idx > MAX_TRANSFERS:
            break
        bad_copy = False
        remote_file = protocol+outdir+f
        if "_scanpoints.txt" in f:
            remote_file = protocol+outdir+sub_dir+f

        print "#"*100
        
        local_sz      = getFileSize(f)
        local_chksum  = getCheckSum(f)
        remote_sz     = getFileSize(remote_file)
        remote_chksum = getCheckSum(remote_file) if remote_sz != -1 else -1

        print "[%d/%d] Transfering File: %s" % (idx+1,len(transfer_files),f)
        if remote_chksum == local_chksum:
            # The file is already present and has correct checksum
            print "\tRemote file already exists, skipping..."
            print ""
            continue

        print "\tTarget: %s" % (remote_file)
        print "\tSize: %.2f MB" % (float(local_sz)/(1024*1024))

        stdout_arr = run_process(['gfal-copy','-fp',f,remote_file])
        copied_chksum = getCheckSum(remote_file)

        result = ""
        if copied_chksum != local_chksum:
            print "\tResult: FAILED"
            failed_copies.append(f)
        else:
            print "\tResult: SUCCESS"
            good_copies.append(f)
        print "\tDone!"

    with open('failed_copies.log','w') as f:
        f.write('failed copies:\n')
        for fname in failed_copies:
            f.write(fname)
            f.write('\n')

    with open('good_copies.log','w') as f:
        f.write('good copies:\n')
        for fname in good_copies:
            f.write(fname)
            f.write('\n')
    return

# Get a list of all files to transfer
def getFilesToTransfer(fdir='.'):
    search_strs = ['_tarball.tar.xz','_scanpoints.txt']
    files = []
    for idx,f in enumerate(os.listdir(fdir)):
        if os.path.isdir(f):
            continue
        elif not isValidOr(f,search_strs):
            # The file does not contain any of the search strings
            continue
        arr = f.split('_')
        if len(arr) < 3:
            continue
        p,c,r = arr[:3]
        cross_checks = [
            "%s_%s_%s_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz" % (p,c,r),
            "%s_%s_%s_scanpoints.txt" % (p,c,r)
        ]
        if not os.path.exists(cross_checks[0]) or not os.path.exists(cross_checks[1]):
            # The file is missing a complement
            continue
        files.append(f)
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

def run_process(inputs,verbose=True):
    p = subprocess.Popen(inputs,stdout=subprocess.PIPE)
    stdout = []
    while True:
        l = p.stdout.readline()
        if l == '' and p.poll() is not None:
            break
        if l:
            stdout.append(l.strip())
            if verbose: print l.strip()
    return stdout


if __name__ == "__main__":
    main()
