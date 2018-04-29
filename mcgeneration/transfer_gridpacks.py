import os
import subprocess

# /tmp/x509up_u92084

# gfal-stat 'gsiftp://deepthought.crc.nd.edu/hadoop/store/user/awightma/gridpack_scans/ctG/ttH_ctG_run2_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz'
# gfal-sum 'gsiftp://deepthought.crc.nd.edu/hadoop/store/user/awightma/gridpack_scans/ctG/ttH_ctG_run2_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz' MD5

MAX_TRANSFERS = 999  # Limit the number of transfers per code running

def main():
    protocol = 'gsiftp://deepthought.crc.nd.edu'
    outdir = '/hadoop/store/user/awightma/gridpack_scans/2018_04_17/'
    search_str = '_tarball.tar.xz'

    failed_copies = []
    good_copies = []

    count = 0
    for idx,f in enumerate(os.listdir('.')):
        if count >= MAX_TRANSFERS:
            break
        bad_copy = False
        if os.path.isdir(f):
            continue
        elif not search_str in f:
            continue

        remote_file = protocol+outdir+f

        local_sz      = getFileSize(f)
        local_chksum  = getCheckSum(f)
        remote_chksum = getCheckSum(remote_file)

        print "[%d] Transfering File: %s" % (idx,f)
        if remote_chksum == local_chksum:
            # The file is already present and has correct checksum
            print "\tSkipping file..."
            print ""
            continue

        print "\tTarget: %s" % (remote_file)
        print "\tSize: %.2f MB" % (float(local_sz)/(1024*1024))

        arr = run_process(['gfal-copy','-fp',f,remote_file])
        copied_chksum = getCheckSum(remote_file)

        result = ""
        if copied_chksum != local_chksum:
            print "\tResult: FAILED"
            failed_copies.append(f)
        else:
            print "\tResult: SUCCESS"
            good_copies.append(f)
        print "\tDone!"
        count += 1

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

def getFileSize(f):
    arr = run_process(['gfal-stat',f],verbose=False)
    if len(arr) < 2:
        return -1

    if not 'Size: ' in arr[1]:
        return -1

    return int(arr[1].split('\t')[0][6:])

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
