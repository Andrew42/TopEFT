import os
import subprocess
import shutil
import itertools
import random
#import numpy as np

# On lxplus requires: scl enable python27 bash

HOME_DIR      = os.getcwd()
CARD_DIR      = os.path.join("addons","cards")           # Relative w.r.t HOME_DIR
LIMITS_DIR    = os.path.join("addons","limits")          # Relative w.r.t HOME_DIR
PROC_CARD_DIR = os.path.join(CARD_DIR,"process_cards")   # Relative w.r.t HOME_DIR

# MadGraph specific card naming
MG_PROC_CARD     = 'proc_card.dat'
MG_CUSTOM_CARD   = 'customizecards.dat'
MG_REWEIGHT_CARD = 'reweight_card.dat'
MG_RUN_CARD      = 'run_card.dat'

CURR_ARCH    = 'slc6_amd64_gcc630'
CURR_RELEASE = 'CMSSW_9_3_0'
GRIDRUN_DIR  = 'gridruns'

SAVE_DIAGRAMS = False   # Note: Need to modify generate_gridpack.sh if set to true (otherwise they get cleaned up)
USE_COUPLING_MODEL = False
COUPLING_STRING = "DIM6^2==1 DIM6_ctW^2==1"

ALL_COEFFS = [
    'ctp','ctpI','cpQM','cpQ3','cpt','cpb','cptb','cptbI','ctW','ctZ','ctWI','ctZI','cbW','cbWI',
    'ctG','ctGI','cQlM1','cQlM2','cQlM3','cQl31','cQl32','cQl33','cQe1','cQe2','cQe3','ctl1',
    'ctl2','ctl3','cte1','cte2','cte3','ctlS1','ctlSI1','ctlS2','ctlSI2','ctlS3','ctlSI3','ctlT1',
    'ctlTI1','ctlT2','ctlTI2','ctlT3','ctlTI3','cblS1','cblSI1','cblS2','cblSI2','cblS3','cblSI3',
    'cQq83','cQq81','cQu8','cQd8','ctq8','ctu8','ctd8','cQq13','cQq11','cQu1','cQd1','ctq1','ctu1',
    'ctd1','cQQ1','cQQ8','cQt1','cQb1','ctt1','ctb1','cQt8','cQb8','ctb8'
]

class BatchType(object):
    LOCAL      = 'local'
    LSF        = 'lsf'
    CMSCONNECT = 'cmsconnect'
    CONDOR     = 'condor'
    NONE       = 'none'

    @classmethod
    def getTypes(cls):
        return [cls.LOCAL,cls.LSF,cls.CMSCONNECT,cls.CONDOR,cls.NONE]

    @classmethod
    def isValid(cls,btype):
        return btype in cls.getTypes()

class ScanType(object):
    FRANDOM   = 'full_random'
    SRANDOM   = '1d_random'
    FLINSPACE = 'full_linspace'
    SLINSPACE = '1d_linspace'
    NONE      = 'none'

    @classmethod
    def getTypes(cls):
        return [cls.FRANDOM,cls.SRANDOM,cls.FLINSPACE,cls.SLINSPACE,cls.NONE]

    @classmethod
    def isValid(cls,stype):
        return stype in cls.getTypes()

def setup_gridpack(template_dir,setup,process,proc_card,limits,num_pts,btype='local',stype='full_linspace'):
    if not BatchType.isValid(btype):
        raise ValueError("%s is not a valid batch type!" % (btype))
    elif not ScanType.isValid(stype):
        raise ValueError("%s is not a valid scan type!" % (stype))
    os.chdir(HOME_DIR)

    tarball = '%s_%s_%s_tarball.tar.xz' % (setup,CURR_ARCH,CURR_RELEASE)
    scanfile = '%s_scanpoints.txt' % (setup)
    if os.path.exists(setup) or os.path.exists(tarball) or os.path.exists(os.path.join(GRIDRUN_DIR,process,setup)):
        print "Skipping gridpack setup: %s" % (setup)
        return 1
    elif os.path.exists(scanfile):
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
    if stype == ScanType.FLINSPACE:
        scan_pts = get_scan_points_linspace(limits,num_pts)
    elif stype == ScanType.FRANDOM:
        scan_pts = get_scan_points_random(limits,num_pts)
    elif stype == ScanType.SLINSPACE:
        scan_pts = get_1d_scan_points_linspace(limits,num_pts)
    elif stype == ScanType.SRANDOM:
        scan_pts = get_1d_scan_points_random(limits,num_pts)
    elif stype == ScanType.NONE:
        scan_pts = []

    save_scan_points(scanfile,limits,scan_pts)

    target_dir = os.path.join(process_subdir,setup)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    else:
        print "\tNOTE: The cards directory already exists, will overwrite existing cards."

    # Copy/Create the cards for the gridpack generation
    shutil.copy(#Customize card
        os.path.join(HOME_DIR,CARD_DIR,template_dir,MG_CUSTOM_CARD),
        os.path.join(target_dir,custom_file)
    )
    shutil.copy(#Run card
        os.path.join(HOME_DIR,CARD_DIR,template_dir,MG_RUN_CARD),
        os.path.join(target_dir,run_file)
    )
    shutil.copy(#Process card
        os.path.join(HOME_DIR,PROC_CARD_DIR,proc_card),
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

    # Needed for using the dim6top_LO_UFO model with FCNC coeffs...
    #run_process(['sed','-i','-e',"s|DIM6=1|FCNC=0 DIM6=1|g","%s/%s" % (target_dir,proc_file)])

    if SAVE_DIAGRAMS:
        # Remove the nojpeg option from the output line
        print "\tSaving diagrams!"
        run_process(['sed','-i','-e',"s|SUBSETUP -nojpeg|SUBSETUP|g","%s/%s" % (target_dir,proc_file)])

    if USE_COUPLING_MODEL:
        print "\tUsing each_coupling_order model!"
        # Replace the default dim6 model with the 'coupling_orders' version
        run_process(['sed','-i','-e',"s|dim6top_LO_UFO|dim6top_LO_UFO_each_coupling_order|g","%s/%s" % (target_dir,proc_file)])
        run_process(['sed','-i','-e',"s|DIM6=1|%s|g" % (COUPLING_STRING),"%s/%s" % (target_dir,proc_file)])

    # Replace SUBSETUP in the process card
    run_process(['sed','-i','-e',"s|SUBSETUP|%s|g" % (setup),"%s/%s" % (target_dir,proc_file)])

    # Run the gridpack generation step
    print "\tGenerating gridpack..."

    if btype == BatchType.LOCAL:
        # For interactive/serial running
        run_process(['./gridpack_generation.sh',setup,target_dir])
    elif btype == BatchType.LSF:
        # For batch running
        run_process(['./submit_gridpack_generation.sh','15000','15000','1nd',setup,target_dir,'8nh'])
    elif btype == BatchType.CMSCONNECT:
        # For cmsconnect running
        debug_file = "%s.debug" % (setup)
        cmsconnect_cores = 1
        print '\t\tCurrent PATH: {0}'.format(os.getcwd())
        print '\t\tWill execute: ./submit_cmsconnect_gridpack_generation.sh {0} {1} {2} "{3}" {4} {5}'.format(setup,target_dir,str(cmsconnect_cores), "15 Gb", CURR_ARCH, CURR_RELEASE)
        subprocess.Popen(
            ["./submit_cmsconnect_gridpack_generation.sh",setup,target_dir,str(cmsconnect_cores),"15 Gb", CURR_ARCH, CURR_RELEASE],
            stdout=open(debug_file,'w'),
            stderr=subprocess.STDOUT
        )
    elif btype == BatchType.CONDOR:
        # Not currently working
        print "Condor running is not currently working. Sorry!"
        #run_process(['./submit_condor_gridpack_generation.sh',setup,target_dir])
    elif btype == BatchType.NONE:
        print "Skipping gridpack generation, %s" % (setup)

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

# Returns a list of linear spaced numbers (implementation of numpy.linspace)
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
def get_scan_points_linspace(limits,num_pts):
    if num_pts == 0:
        return []
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
        arr += [linspace(low,high,num_pts)]
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

# Samples points randomly in the N-dimensional WC space
def get_scan_points_random(limits,num_pts):
    if num_pts == 0:
        return []
    sm_pt    = {}
    start_pt = {}
    for k,(start,low,high) in limits.iteritems():
        sm_pt[k] = 0.0
        start_pt[k] = start
    has_sm_pt = check_point(sm_pt,start_pt)
    rwgt_pts = []
    for idx in range(num_pts):
        pt = {}
        for k,(start,low,high) in limits.iteritems():
            pt[k] = round(random.uniform(low,high),6)
        if check_point(pt,sm_pt):
            has_sm_pt = True
        if check_point(pt,start_pt):
            continue
        rwgt_pts.append(pt)
    if not has_sm_pt:
        rwgt_pts.append(sm_pt)
    return rwgt_pts

# Temporary hacked version to do multiple 1-d scans in a single re-weight
def get_1d_scan_points_random(limits,num_pts):
    if num_pts == 0:
        return []
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
            val = random.uniform(low,high)
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
    if num_pts == 0:
        return []
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

    if len(pts) == 0:
        return file_name

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

# Saves the scan points to a text file formatted into a nice table
def save_scan_points(fpath,limits,rwgt_pts):
    col_spacing = 15
    col_sep = " "
    coeffs = limits.keys()
    with open(fpath,'w') as f:
        header = "".ljust(col_spacing)
        for c in coeffs:
            header += c.ljust(col_spacing) + col_sep
        start_row = "\nMGStart".ljust(col_spacing) + col_sep
        for c in coeffs:
            start_row += str(limits[c][0]).ljust(col_spacing) + col_sep
        f.write(header)
        f.write(start_row)
        for idx,pt in enumerate(rwgt_pts):
            row_name = "rwgt%d" % (idx)
            row = "\n" + row_name.ljust(col_spacing) + col_sep
            for c in coeffs:
                if not pt.has_key(c):
                    row += "0.0".ljust(col_spacing) + col_sep
                else:
                    row += str(pt[c]).ljust(col_spacing) + col_sep
            f.write(row)
    return

def parse_limit_file(fpath):
    wc_limits = {}
    with open(fpath,'r') as f:
        for l in f:
            arr = l.split()
            if len(arr) != 3:
                continue
            wc_limits[arr[0]] = [float(arr[1]),float(arr[2])]
    return wc_limits

####################################################################################################

def main():
    batch_type = BatchType.NONE
    scan_type  = ScanType.NONE

    #batch_type = BatchType.LOCAL
    #batch_type = BatchType.LSF
    #batch_type = BatchType.CMSCONNECT

    scan_type = ScanType.FRANDOM
    scan_type = ScanType.SLINSPACE

    #run_type = 'ndim_singlerun_scan'
    #run_type = 'ndim_multirun_scan'
    #run_type = '1d_multirun_scan'
    run_type = '1d_multi_batch_scan'

    test_gridpack = False

    template_dir = "test_template"
    #template_dir = "ttHJet_template"
    #template_dir = "TopEFTcuts_template"

    proc_card    = "ttH.dat"
    proc_name    = "ttH"

    #proc_card    = "ttll.dat"
    #proc_name    = "ttll"

    #proc_card    = "ttlnu.dat"
    #proc_name    = "ttlnu"

    #proc_card    = "tllq.dat"
    #proc_name    = "tllq"

    grp_tag = '2HvyRef'

    coeff_list = [
        'ctp','cpQM','cpQ3','cpt','cptb','ctW', 'ctZ', 'cbW','ctG',
        #'cQl31','cQlM1','cQe1','ctl1','cte1','ctlS1','ctlT1',
        #'cQl32','cQlM2','cQe2','ctl2','cte2','ctlS2','ctlT2',
        #'cQl33','cQlM3','cQe3','ctl3','cte3','ctlS3','ctlT3',
    ]

    # For multi-run jobs
    num_runs = 7
    rstart,rend = [0,num_runs]

    low_lim  = -20.0
    high_lim =  20.0
    start_pt = -20.0
    num_pts  = 15

    # Determines how close to 0 the randomly sampled WC strength can be
    #NOTE: Range can be [1,inf] and smaller numbers force the point to be further away from 0
    rand_factor = 2

    grid_events = 10000
    seed  = 42
    cores = 1

    MAX_JOBS = 1   # Specifies that maximum number of jobs to try and submit at a time

    random.seed()

    limits_fpath = os.path.join(LIMITS_DIR,"dim6top_LO_UFO_limits.txt")
    wc_limits = parse_limit_file(limits_fpath)

    if batch_type != BatchType.NONE and batch_type != BatchType.LOCAL:
        # Don't let us try to run gridpacks when doing batch production!
        test_gridpack = False

    if run_type == 'ndim_singlerun_scan':
        # Produces a single gridpack which generates enough random rwgt points to extract a quadratic fit
        N = len(coeff_list)
        if scan_type == ScanType.FRANDOM:
            num_pts = 1.2*(1+2*N+N*(N-1)/2)
        elif scan_type == ScanType.SLINSPACE:
            num_pts = 10    # Just needs to be >= 3
        num_pts = int(num_pts)
        print "N-Pts:",num_pts
        run_num = 0
        setup  = "%s_%s_run%d" % (proc_name,grp_tag,run_num)
        limits = {}
        for idx,c in enumerate(coeff_list):
            key = "%s_%s" % (proc_name,c)
            if not wc_limits.has_key(key):
                print "Missing fit limits for %s!" % (key)
                continue
            low  = round(wc_limits[key][0],6)
            high = round(wc_limits[key][1],6)
            start_pt = round(random.uniform(low,high),6)
            counter = 0
            # Make sure the starting point isn't relatively close to 0 for any WC
            while True:
                if counter > 999:
                    raise ValueError("Unable find valid starting point for %s!" % (key))
                if start_pt < 0:
                    if abs(start_pt)*rand_factor > abs(low):
                        break
                else:
                    if abs(start_pt)*rand_factor > abs(high):
                        break
                start_pt = round(random.uniform(low,high),6)
                counter += 1
            limits[c] = [start_pt,low,high]
            print "%s:" % (key.ljust(11)),limits[c]
        setup_gridpack(
            template_dir=template_dir,
            setup=setup,
            process=proc_name,
            proc_card=proc_card,
            limits=limits,
            num_pts=num_pts,
            btype=batch_type,
            stype=scan_type
        )
        if test_gridpack:
            run_gridpack(
                setup=setup,
                process=proc_name,
                events=grid_events,
                seed=seed,
                cores=cores
            )
    elif run_type == 'ndim_multirun_scan':
        # Same as the 'ndim_singlerun_scan', but produces multiple runs for each gridpack setup
        N = len(coeff_list)
        if scan_type == ScanType.FRANDOM:
            num_pts = 1.2*(1+2*N+N*(N-1)/2)
        elif scan_type == ScanType.SLINSPACE:
            num_pts = 10    # Just needs to be >= 3
        num_pts = int(num_pts)
        print "N-Pts:",num_pts
        for run_idx in range(num_runs):
            setup  = "%s_%s_run%d" % (proc_name,grp_tag,run_idx)
            limits = {}
            for idx,c in enumerate(coeff_list):
                key = "%s_%s" % (proc_name,c)
                if not wc_limits.has_key(key):
                    print "Missing fit limits for %s!" % (key)
                    continue
                low  = round(wc_limits[key][0],6)
                high = round(wc_limits[key][1],6)
                start_pt = round(random.uniform(low,high),6)
                counter = 0
                # Make sure the starting point isn't relatively close to 0 for any WC
                while True:
                    if counter > 999:
                        raise ValueError("Unable find valid starting point for %s!" % (key))
                    if start_pt < 0:
                        if abs(start_pt)*rand_factor > abs(low):
                            break
                    else:
                        if abs(start_pt)*rand_factor > abs(high):
                            break
                    start_pt = round(random.uniform(low,high),6)
                    counter += 1
                limits[c] = [start_pt,low,high]
                print "%s:" % (key.ljust(11)),limits[c]
            setup_gridpack(
                template_dir=template_dir,
                setup=setup,
                process=proc_name,
                proc_card=proc_card,
                limits=limits,
                num_pts=num_pts,
                btype=batch_type,
                stype=scan_type
            )
            if test_gridpack:
                run_gridpack(
                    setup=setup,
                    process=proc_name,
                    events=grid_events,
                    seed=seed,
                    cores=cores
                )
    elif run_type == '1d_multirun_scan':
        # Performs a 1-d scan for a particular WC and repeats for multiple MG starting points
        if len(coeff_list) != 1:
            raise ValueError("Invalid coeff_list specified for %s:" % (run_type),coeff_list)
        coeff = coeff_list[0]
        scan_type = ScanType.SLINSPACE    # Force linear spaced sampling
        arr = linspace(low_lim,high_lim,num_runs)
        for idx,a in enumerate(arr):
            if idx < rstart or idx >= rend:
                continue
            start_pt = round(arr[idx],6)
            print "%d: %s" % (idx,start_pt)
            setup  = "%s_%s_run%d" % (proc_name,grp_tag,idx)
            limits = {}
            limits[coeff] = [start_pt,low_lim,high_lim]
            setup_gridpack(
                template_dir=template_dir,
                setup=setup,
                process=proc_name,
                proc_card=proc_card,
                limits=limits,
                num_pts=num_pts,
                btype=batch_type,
                stype=scan_type
            )
            if test_gridpack:
                run_gridpack(
                    setup=setup,
                    process=proc_name,
                    events=grid_events,
                    seed=seed,
                    cores=cores
                )
    elif run_type == '1d_multi_batch_scan':
        # Same as the 1d_multirun_scan, but loops over a list of WC (1 per gridpack)
        count = 0
        scan_type = ScanType.SLINSPACE  # Force linear spaced sampling
        for coeff in coeff_list:
            if count >= MAX_JOBS:
                break
            os.chdir(HOME_DIR)
            arr = linspace(low_lim,high_lim,num_runs)
            for idx,a  in enumerate(arr):
                if count >= MAX_JOBS:
                    break
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
                    btype=batch_type,
                    stype=scan_type
                )
                if test_gridpack:
                    run_gridpack(
                        setup=setup,
                        process=proc_name,
                        events=grid_events,
                        seed=seed,
                        cores=cores
                    )
                if not skipped:
                    count += 1
    print "\nFinished!"

if __name__ == "__main__":
    main()
