import os
import subprocess
import shutil
import itertools
import random
import time
#import numpy as np

from helper_tools import *

# On lxplus using numpy requires: scl enable python27 bash

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

ANALYSIS_COEFFS = [ # As suggested by Adam
    'ctp','cpQM','cpQ3','cpt','cptb','ctW', 'ctZ', 'cbW','ctG',
    'cQl31','cQlM1','cQe1','ctl1','cte1','ctlS1','ctlT1',
    'cQl32','cQlM2','cQe2','ctl2','cte2','ctlS2','ctlT2',
    'cQl33','cQlM3','cQe3','ctl3','cte3','ctlS3','ctlT3',
]

#NOTE: The template directory should contain run_card.dat and customizecards.dat files
PROCESS_MAP = {
    'ttH': {
        'name': 'ttH',
        'process_card': 'ttH.dat',
        'template_dir': 'template_cards/test_template'
    },
    'ttHJet': {
        'name': 'ttH',
        'process_card': 'ttHJet.dat',
        'template_dir': 'template_cards/jets_template'
    },
    'ttHDecay': {
        'name': 'ttH',
        'process_card': 'ttHDecay.dat',
        'template_dir': 'template_cards/test_template'
    },
    'ttW': {
        'name': 'ttW',
        'process_card': 'ttW.dat',
        'template_dir': 'template_cards/test_template'
    },
    'ttZ': {
        'name': 'ttZ',
        'process_card': 'ttZ.dat',
        'template_dir': 'template_cards/test_template'
    },
    'ttll': {
        'name': 'ttll',
        'process_card': 'ttll.dat',
        'template_dir': 'template_cards/test_template'
    },
    'ttllJet': {
        'name': 'ttll',
        'process_card': 'ttllJet.dat',
        'template_dir': 'template_cards/jets_template'
    },
    'ttllDecay': {
        'name': 'ttll',
        'process_card': 'ttllDecay.dat',
        'template_dir': 'template_cards/test_template'
    },
    'ttlnu': {
        'name': 'ttlnu',
        'process_card': 'ttlnu.dat',
        'template_dir': 'template_cards/test_template'
    },
    'ttlnuJet': {
        'name': 'ttlnu',
        'process_card': 'ttlnu.dat',
        'template_dir': 'template_cards/jets_template'
    },
    'ttlnuDecay': {
        'name': 'ttlnu',
        'process_card': 'ttlnuDecay.dat',
        'template_dir': 'template_cards/test_template'
    },
    'tllq': {
        'name': 'tllq',
        'process_card': 'tllq.dat',
        'template_dir': 'template_cards/test_template'
    },
    'tllqJet': {
        'name': 'tllq',
        'process_card': 'tllqJet.dat',
        'template_dir': 'template_cards/jets_template'
    },
    'tllqDecay': {
        'name': 'tllq',
        'process_card': 'tllqDecay.dat',
        'template_dir': 'template_cards/test_template'
    },
}

# Setup/Create the needed folders and files for creating a gridpack
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
        return
    elif os.path.exists(scanfile):
        print "Skipping gridpack setup: %s" % (setup)
        return
    
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
    scan_pts = ScanType.getPoints(limits,num_pts,stype)

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
    make_gridpack(setup,target_dir,btype)

    return

# Create a gridpack using a specific BatchType
def make_gridpack(setup,target_dir,btype):
    if not os.path.exists(target_dir):
        print "[ERROR] Can't find target directory, %s" % (target_dir)
        return
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

# Unpack and run an existing gridpack to produce events
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

# Reads a limit file and returns a dictionary mapping the WCs to their respective high,low limits to use
def parse_limit_file(fpath):
    wc_limits = {}
    with open(fpath,'r') as f:
        for l in f:
            arr = l.split()
            if len(arr) != 3:
                continue
            wc_limits[arr[0]] = [float(arr[1]),float(arr[2])]
    return wc_limits

# Calculates a random point between two specified values
def calculate_start_point(low,high,rfact=1.25):
    #NOTE1: rfact determines how close to 0 the randomly sample WC stregnth can be
    #NOTE2: rfact range can be [1,inf] and smaller numbers force the point to be further away from 0
    max_attempts = 999
    counter = 0
    #rand_factor = 1.25
    start_pt = round(random.uniform(low,high),6)
    while True:
        if counter > max_attempts:
            raise ValueError("Unable find valid starting point!")
        if start_pt < 0:
            if abs(start_pt)*rfact > abs(low):
                break
        else:
            if abs(start_pt)*rfact > abs(high):
                break
        start_pt = round(random.uniform(low,high),6)
        counter += 1
    return start_pt

def submit_gridpack(ops):
    batch_type = ops['batch_type']
    scan_type  = ops['scan_type']
    proc_name  = ops['process']
    grp_tag    = ops['tag']
    run_num    = ops['run']
    coeff_list = ops['coeffs']
    start_pt   = ops['start_pt']
    num_pts    = ops['rwgt_pts']

    if not PROCESS_MAP.has_key(proc_name):
        print "Unknown process: %s" % (proc_name)
        return
    proc_card    = PROCESS_MAP[proc_name]['process_card']
    template_dir = PROCESS_MAP[proc_name]['template_dir']
    limits_name  = PROCESS_MAP[proc_name]['name']

    test_gridpack = False
    grid_events = 10000
    seed  = 42
    cores = 1

    random.seed()

    limits_fpath = os.path.join(LIMITS_DIR,"dim6top_LO_UFO_limits.txt")
    wc_limits    = parse_limit_file(limits_fpath)
    setup_name   = "%s_%s_run%d" % (proc_name,grp_tag,run_num)

    if batch_type != BatchType.NONE and batch_type != BatchType.LOCAL:
        # Don't let us try to run gridpacks when doing batch production!
        test_gridpack = False

    N = len(coeff_list)
    if scan_type == ScanType.FRANDOM:
        num_pts = max(num_pts,1.2*(1+2*N+N*(N-1)/2))
    elif scan_type == ScanType.SLINSPACE:
        num_pts = max(num_pts,3)
    num_pts = int(num_pts)
    print "N-Pts:",num_pts
    limits = {}
    for idx,c in enumerate(coeff_list):
        key = "%s_%s" % (limits_name,c)
        if wc_limits.has_key(key):
            low  = round(wc_limits[key][0],6)
            high = round(wc_limits[key][1],6)
        else:
            #print "Missing fit limits for %s!" % (key)
            low  = -10.0
            high = 10.0
        if start_pt.has_key(c):
            strength = start_pt[c]
        else:
            strength = calculate_start_point(low,high,1.25)
        limits[c] = [strength,low,high]
        print "%s:" % (key.ljust(11)),limits[c]
    setup_gridpack(
        template_dir=template_dir,
        setup=setup_name,
        process=proc_name,
        proc_card=proc_card,
        limits=limits,
        num_pts=num_pts,
        btype=batch_type,
        stype=scan_type
    )
    if test_gridpack:
        run_gridpack(
            setup=setup_name,
            process=proc_name,
            events=grid_events,
            seed=seed,
            cores=cores
        )

####################################################################################################

def main():
    options = {
        'batch_type': BatchType.NONE,
        'scan_type': ScanType.NONE,
        'process': '',
        'tag': '',
        'run': 0,
        'coeffs': [],
        'start_pt': {},
        'rwgt_pts': 10,
    }

    options['batch_type'] = BatchType.NONE
    #options['scan_type']  = ScanType.FLINSPACE
    options['scan_type']  = ScanType.FRANDOM
    options['tag']        = 'ExampleTest'
    options['rwgt_pts']   = 10
    options['coeffs']     = [
        'ctW','ctp','cpQM','ctZ','ctG','cbW','cpQ3','cptb','cpt',
        'cQl31','cQlM1','cQe1','ctl1','cte1','ctlS1','ctlT1',
    ]

    if options['scan_type'] == ScanType.SLINSPACE:
        options['tag'] = options['tag'] + "AxisScan"
    elif options['scan_type'] == ScanType.FRANDOM:
        options['tag'] = options['tag'] + "FullScan"

    starting_points = [
        {
            'ctW':  -10.425061,
            'ctp':  51.017823,
            'cpQM': -127.460382,
            'ctZ':  -12.37404,
            'ctG':  3.088479,
            'cbW':  48.942369,
            'cpQ3': -42.329907,
            'cptb': -105.381412,
            'cpt':  -145.130401,
        },
        {
            'ctW':  9.230491,
            'ctp':  -18.412743,
            'cpQM': 127.523771,
            'ctZ':  -12.700832,
            'ctG':  3.088479,
            'cbW':  48.942369,
            'cpQ3': -42.329907,
            'cptb': -105.381412,
            'cpt':  -145.130401,
        },
        {
            'ctW':  -10.425061,
            'ctp':  51.017823,
            'cpQM': -127.460382,
            'ctZ':  -12.37404,
            'ctG':  -2.894448,
            'cbW':  44.600086,
            'cpQ3': -43.06341,
            'cptb': -92.81369,
            'cpt':  -149.972186,
        },
        {
            'ctW':  9.230491,
            'ctp':  -18.412743,
            'cpQM': 127.523771,
            'ctZ':  -12.700832,
            'ctG':  -2.894448,
            'cbW':  44.600086,
            'cpQ3': -43.06341,
            'cptb': -92.81369,
            'cpt':  -149.972186,
        },
        {
            'ctW':  -10.425061,
            'ctp':  51.017823,
            'cpQM': -127.460382,
            'ctZ':  -12.37404,
            'ctG':  -2.882015,
            'cbW':  -47.663293,
            'cpQ3': -40.25991,
            'cptb': -93.482367,
            'cpt':  136.026875,
        },
        {
            'ctW':  9.230491,
            'ctp':  -18.412743,
            'cpQM': 127.523771,
            'ctZ':  -12.700832,
            'ctG':  -2.882015,
            'cbW':  -47.663293,
            'cpQ3': -40.25991,
            'cptb': -93.482367,
            'cpt':  136.026875,
        }
    ]

    #options['rwgt_pts'] = 3
    #options['coeffs'] = ['ctW','ctp','cpQM']
    #starting_points = [{} for x in range(2)]
    starting_points = [{}]

    proc_list = ['ttH']
    delay = 0.5
    for p in proc_list:
        options['process'] = p
        for idx,pt in enumerate(starting_points):
            options['run'] = idx
            options['start_pt'] = pt
            submit_gridpack(ops=options)
            time.sleep(delay)

if __name__ == "__main__":
    main()
    print "\nFinished!"