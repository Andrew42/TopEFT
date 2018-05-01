import os
import subprocess
import shutil
import itertools
import numpy as np

# On lxplus requires: scl enable python27 bash

HOME_DIR = os.getcwd()
CARD_DIR = os.path.join("addons","cards")           # Relative w.r.t HOME_DIR
PROC_DIR = os.path.join(CARD_DIR,"process_cards")   # Relative w.r.t HOME_DIR

# MadGraph specific card naming
MG_PROC_CARD     = 'proc_card.dat'
MG_CUSTOM_CARD   = 'customizecards.dat'
MG_REWEIGHT_CARD = 'reweight_card.dat'
MG_RUN_CARD      = 'run_card.dat'

CURR_ARCH    = 'slc6_amd64_gcc630'
CURR_RELEASE = 'CMSSW_9_3_0'
GRIDRUN_DIR  = 'gridruns'

class RunType(object):
    LOCAL      = 'local'
    LSF        = 'lsf'
    CMSCONNECT = 'cmsconnect'
    CONDOR     = 'condor'

    @classmethod
    def getTypes(cls):
        return [cls.LOCAL,cls.LSF,cls.CMSCONNECT,cls.CONDOR]

    @classmethod
    def isValid(cls,rtype):
        return rtype in cls.getTypes()

def setup_gridpack(template_dir,setup,process,proc_card,limits,num_pts,rtype='local'):
    if not RunType.isValid(rtype):
        raise ValueError("%s is not a valid run type!" % (rtype))
    os.chdir(HOME_DIR)

    tarball = '%s_%s_%s_tarball.tar.xz' % (setup,CURR_ARCH,CURR_RELEASE)
    if os.path.exists(setup) or os.path.exists(tarball) or os.path.exists(os.path.join(GRIDRUN_DIR,process,setup)):
        print "Skipping gridpack setup: %s" % (setup)
        return 1
    
    print "Creating Gridpack: %s" % (setup)
    print "\tSetting up cards..."
    # Directory were we will put all the different setup cards for a particluar process
    process_subdir = os.path.join(CARD_DIR,"%s_cards" % (process))
    if not os.path.exists(process_subdir):
        os.mkdir(process_subdir)

    # Gridpack specific file names
    rwgt_file    = "%s_%s" % (setup,MG_REWEIGHT_CARD)
    custom_file  = "%s_%s" % (setup,MG_CUSTOM_CARD)
    run_file     = "%s_%s" % (setup,MG_RUN_CARD)
    proc_file    = "%s_%s" % (setup,MG_PROC_CARD)

    # Coefficient scan points
    scan_pts = get_scan_points(limits,num_pts)
    #scan_pts = get_1d_scan_points_random(limits,num_pts)
    #scan_pts = get_1d_scan_points_linspace(limits,num_pts)

    target_dir = os.path.join(process_subdir,setup)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    else:
        print "\tNOTE: The cards directory already exists, will overwrite existing cards."

    # Copy/Create the cards for the gridpack generation
    shutil.copy(
        os.path.join(HOME_DIR,CARD_DIR,template_dir,MG_CUSTOM_CARD),
        os.path.join(target_dir,custom_file)
    )
    shutil.copy(
        os.path.join(HOME_DIR,CARD_DIR,template_dir,MG_RUN_CARD),
        os.path.join(target_dir,run_file)
    )
    shutil.copy(
        os.path.join(HOME_DIR,PROC_DIR,proc_card),
        os.path.join(target_dir,proc_file)
    )

    # Create/Modify cards for gridpack generation
    set_initial_point(
        os.path.join(target_dir,custom_file),
        limits
    )
    make_reweight_card(
        os.path.join(target_dir,rwgt_file),
        scan_pts
    )

    # Replace SUBSTEP in the process card
    #p = subprocess.check_output(['sed','-i','-e',"s|SUBSETUP|%s|g" % (setup),"%s/%s" % (target_dir,proc_file)])    # Not present in python2.6
    run_process(['sed','-i','-e',"s|SUBSETUP|%s|g" % (setup),"%s/%s" % (target_dir,proc_file)])


    # Run the gridpack generation step
    print "\tGenerating gridpack..."

    if rtype == RunType.LOCAL:
        # For interactive/serial running
        run_process(['./gridpack_generation.sh',setup,target_dir])
    elif rtype == RunType.LSF:
        # For batch running
        run_process(['./submit_gridpack_generation.sh','15000','15000','1nd',setup,target_dir,'8nh'])
    elif rtype == RunType.CMSCONNECT:
        # For cmsconnect running
        debug_file = "%s.debug" % (setup)
        cmsconnect_cores = 1
        print '\t\tCurrent PATH: {0}'.format(os.getcwd())
        print '\t\tDir: {0}'.format(os.listdir("."))
        print '\t\tWill execute: ./submit_cmsconnect_gridpack_generation.sh {0} {1} {2} "{3}" {4} {5}'.format(setup,target_dir,str(cmsconnect_cores), "15 Gb", CURR_ARCH, CURR_RELEASE)
        subprocess.Popen(
            ["./submit_cmsconnect_gridpack_generation.sh",setup,target_dir,str(cmsconnect_cores),"15 Gb", CURR_ARCH, CURR_RELEASE],
            stdout=open(debug_file,'w'),
            stderr=subprocess.STDOUT
        )
    elif rtype == RunType.CONDOR:
        # Not currently working
        print "Condor running is not currently working. Sorry!"
        #run_process(['./submit_condor_gridpack_generation.sh',setup,target_dir])

    return 0

def run_gridpack(setup,process,events,seed,cores):
    os.chdir(HOME_DIR)

    print "Running Gridpack: %s" % (setup)
    print "\tSetting up directories..."

    if not os.path.exists(GRIDRUN_DIR):
        os.mkdir(GRIDRUN_DIR)

    process_subdir = os.path.join(GRIDRUN_DIR,process)
    if not os.path.exists(process_subdir):
        os.mkdir(process_subdir)

    tarball = '%s_%s_%s_tarball.tar.xz' % (setup,CURR_ARCH,CURR_RELEASE)
    output_dir = os.path.join(process_subdir,'%s' % (setup))
    if os.path.exists(output_dir):
        print "Output directory already exists, skipping gridpack run: %s" % (setup)
        return
    else:
        os.mkdir(output_dir)

    print "\tMoving tarball..."
    shutil.move(tarball,output_dir)
    #shutil.copy(tarball,output_dir)

    os.chdir(output_dir)

    print "\tExtracting tarball..."
    run_process(['tar','xaf',tarball])

    print "\tRunning gridpack..."
    run_process(['./runcmsgrid.sh',str(events),str(seed),str(cores)])

    return

####################################################################################################

# Pipes subprocess messages to STDOUT
def run_process(inputs):
    p = subprocess.Popen(inputs,stdout=subprocess.PIPE)
    while True:
        l = p.stdout.readline()
        if l == '' and p.poll() is not None:
            break
        if l:
            print l.strip()
    return

# Returns a list of linear spaced numbers
def linspace(start,stop,num,endpoint=True,acc=7):
    if num < 0:
        raise ValueError("Number of samples, %s, must be non-negative." % num)
    acc = max(0,acc)
    acc = min(15,acc)
    div = (num - 1) if endpoint else num
    delta = stop - start
    if num > 1:
        step = delta / div
        y = [round((start + step*idx),acc) for idx in range(num)]
    elif num == 1:
        y = [start]
    else:
        y = []
    if endpoint and num > 1:
        y[-1] = stop
    return y

# This works for multi-dim scans now
def get_scan_points(limits,num_pts):
    sm_pt = {}
    for k in limits.keys():
        sm_pt[k] = 0.0
    start_pt = {}
    for k,arr in limits.iteritems():
        start_pt[k] = arr[0]
    has_sm_pt = check_point(sm_pt,start_pt)
    rwgt_pts  = []
    coeffs    = []
    arr       = []
    for k,(start,low,high) in limits.iteritems():
        coeffs.append(k)
        #arr += [np.linspace(low,high,num_pts)]
        arr += [linspace(low,high,num_pts)]
    #mesh_pts = cartesian_product(*arr)
    mesh_pts = [a for a in itertools.product(*arr)]
    for rwgt_pt in mesh_pts:
        pt = {}
        for idx,k in enumerate(coeffs):
            pt[k] = round(rwgt_pt[idx],6)
        if check_point(pt,sm_pt):
            # Skip SM point
            has_sm_pt = True
        if check_point(pt,start_pt):
            # Skip starting point
            continue
        rwgt_pts.append(pt)
    if not has_sm_pt:
        rwgt_pts.append(sm_pt)
    return rwgt_pts

# Temporary hacked version to do multiple 1-d scans in a single re-weight
def get_1d_scan_points_random(limits,num_pts):
    sm_pt    = {}
    start_pt = {}
    for k,(start,low,high) in limits.iteritems():
        sm_pt[k] = 0.0
        start_pt[k] = start
    has_sm_pt = check_point(sm_pt,start_pt)
    rwgt_pts = []
    for k1,(start,low,high) in limits.iteritems():
        for idx in range(num_pts):
            pt = {}
            val = np.random.uniform(low,high)
            pt[k1] = round(val,6)
            for k2 in limits.keys():
                if k1 == k2:
                    continue
                else:
                    pt[k1] = 0.0
            if check_point(pt,sm_pt):
                has_sm_pt = True
            if check_point(pt,start_pt):
                continue
            rwgt_pts.append(pt)
    if not has_sm_pt:
        rwgt_pts.append(sm_pt)
    return rwgt_pts

# Temporary hacked version to do multiple 1-d scans in a single re-weight with linear spacing
def get_1d_scan_points_linspace(limits,num_pts):
    sm_pt = {}
    for k in limits.keys():
        sm_pt[k] = 0.0
    rwgt_pts  = []
    start_pt  = {}
    coeffs    = []
    for k,arr in limits.iteritems():
        start_pt[k] = arr[0]
        coeffs.append(k)
    has_sm_pt = check_point(sm_pt,start_pt)
    for k1 in coeffs:
        #arr = np.linspace(limits[k1][1],limits[k1][2],num_pts)
        arr = linspace(limits[k1][1],limits[k1][2],num_pts)
        for val in arr:
            pt = {}
            for k2 in coeffs:
                if k1 == k2:
                    pt[k2] = round(val,6)
                else:
                    pt[k2] = 0.0
            if check_point(pt,sm_pt):
                has_sm_pt = True
            if check_point(pt,start_pt):
                continue
            rwgt_pts.append(pt)
    if not has_sm_pt:
        rwgt_pts.append(sm_pt)
    return rwgt_pts

# Checks if two W.C. phase space points are identical
def check_point(pt1,pt2):
    for k,v in pt1.iteritems():
        if not pt2.has_key(k):
            pt2[k] = 0.0    # pt2 is missing the coeff, add it and set it to SM value
        if v != pt2[k]:
            return False
    return True

# Perform a cartesian product
#def cartesian_product(*arrays):
#    # https://stackoverflow.com/questions/11144513
#    la = len(arrays)
#    #dtype = np.result_type(*arrays)    # Not present in numpy 1.4
#    dtype = np.find_common_type([arr.dtype for arr in arrays], [])
#    arr = np.empty([len(a) for a in arrays] + [la], dtype=dtype)
#    for i, a in enumerate(np.ix_(*arrays)):
#        arr[..., i] = a
#    return arr.reshape(-1, la)

# Sets the initial W.C. phase space point for MadGraph to start from
def set_initial_point(file_name,limits):
    with open(file_name,'a') as f:
        f.write('set param_card MB 0.0 \n')
        f.write('set param_card ymb 0.0 \n')
        for k,arr in limits.iteritems():
            f.write('set param_card %s %.6f \n' % (k,arr[0]))
        f.write('\n')
    return file_name

# Create the MadGraph reweight card with scans over the specified W.C. phase space points
def make_reweight_card(file_name,pts):
    # pts = [{c1: 1.0, c2: 1.0, ...}, {c1: 1.0, c2: 2.0, ...}, ...]
    header  = ""
    header += "#******************************************************************\n"
    header += "#                       Reweight Module                           *\n"
    header += "#******************************************************************\n"
    header += "\nchange rwgt_dir rwgt\n"

    with open(file_name,'w') as f:
        f.write(header)
        for idx,p in enumerate(pts):
            if idx == 0:
                # This is a workaround for the MG bug causing first point to not be renamed
                f.write('\nlaunch --rwgt_name=dummy_point')
                f.write('\nset %s 0.0123' % (p.keys()[0]))
                f.write('\n')
            rwgt_str = 'EFTrwgt%d' % (idx)
            for k,v in p.iteritems():
                rwgt_str += '_' + k + '_' + str(round(v,6))
            f.write('\nlaunch --rwgt_name=%s' % (rwgt_str))
            for k,v in p.iteritems():
                f.write('\nset %s %.6f' % (k,v))
            f.write('\n')
    return file_name

####################################################################################################

def main():
    batch_running = False
    run_type = RunType.LOCAL

    template_dir = "test_template"
    #template_dir = "ttHJet_template"
    #template_dir = "TopEFTcuts_template"

    #proc_card    = "ttbar.dat"
    #proc_name    = "ttbar"

    proc_card    = "ttll.dat"
    proc_name    = "ttll"

    #proc_card    = "ttlnu.dat"
    #proc_name    = "ttlnu"

    #proc_card    = "tllq.dat"
    #proc_name    = "tllq"

    low_lim  = -10.0
    high_lim =  10.0
    start_pt = 10.0
    num_pts  = 11

    grid_events = 10000
    seed  = 42
    cores = 1

    if batch_running:
        MAX_JOBS = 100   # Specifies that maximum number of jobs to try and submit at a time
        coeff_list = [
            'ctp','ctpI','cpQM','cpQ3','cpt','cpb','cptb','cptbI','ctW','ctZ','ctWI','ctZI','cbW','cbWI',
            'ctG','ctGI','cQlM1','cQlM2','cQlM3','cQl31','cQl32','cQl33','cQe1','cQe2','cQe3','ctl1',
            'ctl2','ctl3','cte1','cte2','cte3','ctlS1','ctlSI1','ctlS2','ctlSI2','ctlS3','ctlSI3','ctlT1',
            'ctlTI1','ctlT2','ctlTI2','ctlT3','ctlTI3','cblS1','cblSI1','cblS2','cblSI2','cblS3','cblSI3',
            'cQq83','cQq81','cQu8','cQd8','ctq8','ctu8','ctd8','cQq13','cQq11','cQu1','cQd1','ctq1','ctu1',
            'ctd1','cQQ1','cQQ8','cQt1','cQb1','ctt1','ctb1','cQt8','cQb8','ctb8'
        ]

        #coeff_list = ['ctt1','cblSI1']

        count = 0
        for coeff in coeff_list:
            if count > MAX_JOBS:
                break
            os.chdir(HOME_DIR)
            #arr = np.linspace(low_lim,high_lim,3)
            arr = linspace(low_lim,high_lim,3)
            for idx,a  in enumerate(arr):
                start_pt = round(arr[idx],6)
                print "%d: %s" % (idx,start_pt)
                setup  = "%s_%s_run%d" % (proc_name,coeff,idx)
                limits = {
                    coeff: [start_pt,low_lim,high_lim]
                }

                skipped = setup_gridpack(
                    template_dir=template_dir,
                    setup=setup,
                    process=proc_name,
                    proc_card=proc_card,
                    limits=limits,
                    num_pts=num_pts,
                    rtype=run_type
                )

                if not skipped:
                    count += 1
    else:
        # Configurable parameters
        coeff = 'ctG'
        #arr = np.linspace(low_lim,high_lim,3)
        arr = linspace(low_lim,high_lim,3)
        for idx,a in enumerate(arr):
            #if idx != 1:
            #    continue
            start_pt = round(arr[idx],6)
            print "%d: %s" % (idx,start_pt)
            setup  = "%s_%s_run%d" % (proc_name,coeff,idx)
            limits = {}
            limits['ctG'] = [start_pt,low_lim,high_lim]
            #limits['cpQ3'] = [7.8,low_lim,high_lim]
            #limits['ctZI'] = [9.8,low_lim,high_lim]
            #limits['cpt']  = [-15.3,low_lim*3,high_lim*3]
            #limits['cpQM'] = [-12.4,low_lim*3,high_lim*3]

            setup_gridpack(
                template_dir=template_dir,
                setup=setup,
                process=proc_name,
                proc_card=proc_card,
                limits=limits,
                num_pts=num_pts,
                rtype=run_type
            )
        
            #run_gridpack(
            #    setup=setup,
            #    process=proc_name,
            #    events=grid_events,
            #    seed=seed,
            #    cores=cores
            #)

            break

    print "\nFinished!"

if __name__ == "__main__":
    main()
