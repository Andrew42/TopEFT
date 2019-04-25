import os
import subprocess
import shutil
import itertools
import random
import time

from helpers.helper_tools import linspace, parse_limit_file, find_process
from helpers.ScanType import ScanType
from helpers.BatchType import BatchType
from helpers.DegreeOfFreedom import DegreeOfFreedom
from helpers.JobTracker import JobTracker
from helpers.Gridpack import Gridpack
from helpers.MGProcess import MGProcess

#voms-proxy-init -voms cms -valid 192:00
#python configure_gridpack.py >& output.log &

#NOTE: The template directory should contain run_card.dat and customizecards.dat files
ttH      = MGProcess(name='ttH'     ,process='ttH'   ,pcard='ttH.dat'     ,tdir='EFT-ttH_template')
ttll     = MGProcess(name='ttll'    ,process='ttll'  ,pcard='ttll.dat'    ,tdir='EFT-ttll_template')
ttlnu    = MGProcess(name='ttlnu'   ,process='ttlnu' ,pcard='ttlnu.dat'   ,tdir='EFT-ttlnu_template')
tllq     = MGProcess(name='tllq'    ,process='tllq'  ,pcard='tllq.dat'    ,tdir='EFT-tllq_template')
tHq      = MGProcess(name='tHq'     ,process='tHq'   ,pcard='tHq.dat'     ,tdir='EFT-tHq_template')
ttHJet   = MGProcess(name='ttHJet'  ,process='ttH'   ,pcard='ttHJet.dat'  ,tdir='jets_template')
ttHDecay = MGProcess(name='ttHDecay',process='ttH'   ,pcard='ttHDecay.dat',tdir='defaultPDFs_template')
ttbar    = MGProcess(name='ttbar'   ,process='ttbar' ,pcard='ttbar.dat'   ,tdir='defaultPDFs_template')
tHlnu    = MGProcess(name='tHlnu'   ,process='tHlnu' ,pcard='tHlnu.dat'   ,tdir='centralTHW_template')
ttWlnu   = MGProcess(name='ttWlnu'  ,process='ttWlnu',pcard='ttWlnu.dat'  ,tdir='centralTTWW_template')
tttt     = MGProcess(name='tttt'    ,process='tttt'  ,pcard='tttt.dat'    ,tdir='tttt_template')

ctp   = DegreeOfFreedom(name='ctp'  ,relations=[['ctp'] ,1.0])
cpQM  = DegreeOfFreedom(name='cpQM' ,relations=[['cpQM'],1.0])
cpQ3  = DegreeOfFreedom(name='cpQ3' ,relations=[['cpQ3'],1.0])
cpt   = DegreeOfFreedom(name='cpt'  ,relations=[['cpt'] ,1.0])
cptb  = DegreeOfFreedom(name='cptb' ,relations=[['cptb'],1.0])
ctW   = DegreeOfFreedom(name='ctW'  ,relations=[['ctW'] ,1.0])
ctZ   = DegreeOfFreedom(name='ctZ'  ,relations=[['ctZ'] ,1.0])
cbW   = DegreeOfFreedom(name='cbW'  ,relations=[['cbW'] ,1.0])
ctG   = DegreeOfFreedom(name='ctG'  ,relations=[['ctG'] ,1.0])
cQei  = DegreeOfFreedom(name='cQei' ,relations=[['cQe1','cQe2','cQe3'],1.0])
ctli  = DegreeOfFreedom(name='ctli' ,relations=[['ctl1','ctl2','ctl3'],1.0])
ctei  = DegreeOfFreedom(name='ctei' ,relations=[['cte1','cte2','cte3'],1.0])
cQl3i = DegreeOfFreedom(name='cQl3i',relations=[['cQl31','cQl32','cQl33'],1.0])
cQlMi = DegreeOfFreedom(name='cQlMi',relations=[['cQlM1','cQlM2','cQlM3'],1.0])
ctlSi = DegreeOfFreedom(name='ctlSi',relations=[['ctlS1','ctlS2','ctlS3'],1.0])
ctlTi = DegreeOfFreedom(name='ctlTi',relations=[['ctlT1','ctlT2','ctlT3'],1.0])

# Four heavy quarks
cQQ1   = DegreeOfFreedom(name='cQQ1'   ,relations=[['cQQ1'],1.0])
cQQ8   = DegreeOfFreedom(name='cQQ8'   ,relations=[['cQQ8'],1.0])
cQt1   = DegreeOfFreedom(name='cQt1'   ,relations=[['cQt1'],1.0])
cQt8   = DegreeOfFreedom(name='cQt8'   ,relations=[['cQt8'],1.0])
cQb1   = DegreeOfFreedom(name='cQb1'   ,relations=[['cQb1'],1.0])
cQb8   = DegreeOfFreedom(name='cQb8'   ,relations=[['cQb8'],1.0])
ctt1   = DegreeOfFreedom(name='ctt1'   ,relations=[['ctt1'],1.0])
ctb1   = DegreeOfFreedom(name='ctb1'   ,relations=[['ctb1'],1.0])
cQtQb1 = DegreeOfFreedom(name='cQtQb1' ,relations=[['cQtQb1'],1.0])
cQtQb8 = DegreeOfFreedom(name='cQtQb8' ,relations=[['cQtQb8'],1.0])

# Two-heavy-two-light quarks
cQq13 = DegreeOfFreedom(name='cQq13' ,relations=[['cQq13'],1.0])
cQq83 = DegreeOfFreedom(name='cQq83' ,relations=[['cQq83'],1.0])
cQq11 = DegreeOfFreedom(name='cQq11' ,relations=[['cQq11'],1.0])
cQq81 = DegreeOfFreedom(name='cQq81' ,relations=[['cQq81'],1.0])
cQu1  = DegreeOfFreedom(name='cQu1'  ,relations=[['cQu1'],1.0])
cQu8  = DegreeOfFreedom(name='cQu8'  ,relations=[['cQu8'],1.0])
cQd1  = DegreeOfFreedom(name='cQd1'  ,relations=[['cQd1'],1.0])
cQd8  = DegreeOfFreedom(name='cQd8'  ,relations=[['cQd8'],1.0])
ctq1  = DegreeOfFreedom(name='ctq1'  ,relations=[['ctq1'],1.0])
ctq8  = DegreeOfFreedom(name='ctq8'  ,relations=[['ctq8'],1.0])
ctu1  = DegreeOfFreedom(name='ctu1'  ,relations=[['ctu1'],1.0])
ctu8  = DegreeOfFreedom(name='ctu8'  ,relations=[['ctu8'],1.0])
ctd1  = DegreeOfFreedom(name='ctd1'  ,relations=[['ctd1'],1.0])
ctd8  = DegreeOfFreedom(name='ctd8'  ,relations=[['ctd8'],1.0])

all_coeffs       = [ctp,cpQM,cpQ3,cpt,cptb,ctW,ctZ,cbW,ctG,cQQ1,cQQ8,cQt1,cQt8,ctt1,cQei,ctli,ctei,cQl3i,cQlMi,ctlSi,ctlTi]
ana_coeffs       = [ctp,cpQM,ctW,ctZ,ctG,cbW,cpQ3,cptb,cpt,cQl3i,cQlMi,cQei,ctli,ctei,ctlSi,ctlTi]  # 16 operators
coeffs_4Hvy      = [cQQ1,cQQ8,cQt1,cQt8,ctt1,ctb1,cQtQb1,cQtQb8]    # 8 operators
coeffs_2Hvy_2Lgt = [cQq13,cQq83,cQq11,cQq81,cQu1,cQu8,cQd1,cQd8,ctq1,ctq8,ctu1,ctu8,ctd1,ctd8]  # 14 operators

# For submitting many gridpack jobs on cmsconnect
def cmsconnect_chain_submit(dofs,proc_list,tag_postfix,rwgt_pts,runs,stype,scan_files=[],proc_run_wl={}):
    #NOTE: The proc_run_wl is only use for SLINSPACE mode
    if runs == 0:
        print "ERROR: For Batch jobs, need to specify at least 1 run!"
        return

    tracker = JobTracker(fdir=os.getcwd())
    max_gen = 5         # Max number of CODEGEN jobs to have running
    max_int = 7         # Max number of INTEGRATE jobs to have running
    max_run = 50        # Max number of total jobs running
    int_cut = 45*60     # Time (relative to INTEGRATE step) before additional jobs can get submitted
    tar_cut = 5*60      # Time the tarball needs to go without being modified to qualify as complete
    delay = 5.0*60      # Time between checks
    tracker.setIntegrateCutoff(int_cut)
    tracker.setTarballCutoff(tar_cut)
    proc_filter = ["^%s$" % (x.getName()) for x in proc_list]
    tags_filter = ["^%s$" % (tag_postfix)] + ["^%s%s$" % (x.getName(),tag_postfix) for x in dofs]
    tracker.setJobFilters(procs=proc_filter,tags=tags_filter)
    done = False
    while not done:
        tracker.update()
        tracker.showJobs(wl=[JobTracker.CODEGEN,JobTracker.INTEGRATE])
        tracker.checkProgress()
        print ""
        max_submits = min(
            max_gen - len(tracker.codegen),
            max_int - len(tracker.intg_filter),
            max_run - len(tracker.running)
        )
        if max_submits <= 0:
            # There are to many jobs already running, wait for some to finish
            time.sleep(delay)
            continue

        for job in tracker.finished:
            if tracker.getTarballTime(job) > 3*(tar_cut+delay):
                # Skip checking jobs that finished sufficiently long ago
                continue
            p,c,r = job.split('_')
            p_obj = find_process(p,proc_list)
            if p_obj is None:
                continue 
            tmp_gp = Gridpack(process=p_obj)
            tmp_gp.ops['tag'] = c
            tmp_gp.ops['run'] = int(r[3:])  # Need to get the integer value from the run string
            if not tmp_gp.exists():
                continue
            tmp_gp.is_configured = True
            if tracker.logHasError(job=job,fdir='.') or not tracker.logHasXsec(job=job,fdir='.'):
                tmp_gp.clean()

        submitted = 0
        for p in proc_list:
            gridpack = Gridpack(
                process=p,
                stype=stype,
                btype=BatchType.CMSCONNECT
            )
            #TODO: Might not want to split it up like this
            if stype == ScanType.SLINSPACE:
                if proc_run_wl.has_key(p.getName()):
                    submitted += submit_1dim_jobs(
                        gp=gridpack,
                        dofs=dofs,
                        npts=rwgt_pts,
                        runs=runs,
                        tag_postfix=tag_postfix,
                        max_submits=max_submits,
                        run_wl=proc_run_wl[p.getName()]
                    )
                else:
                    submitted += submit_1dim_jobs(
                        gp=gridpack,
                        dofs=dofs,
                        npts=rwgt_pts,
                        runs=runs,
                        tag_postfix=tag_postfix,
                        max_submits=max_submits
                    )
            elif stype == ScanType.FRANDOM:
                start_pts = []
                for idx in range(runs):
                    # Note: This means all runs will have the same MadGraph starting point
                    pt = {}
                    for dof in dofs:
                        pt[dof.getName()] = 4.0 # Robert starting value
                    start_pts.append(pt)
                submitted += submit_ndim_jobs(
                    gp=gridpack,
                    dofs=dofs,
                    npts=rwgt_pts,
                    runs=runs,
                    tag=tag_postfix,
                    start_pts=start_pts,
                    max_submits=max_submits
                )
            elif stype == ScanType.FROMFILE:
                submitted += submit_scanfile_jobs(
                    gp=gridpack,
                    dofs=dofs,
                    tag=tag_postfix,
                    scan_files=scan_files,
                    max_submits=max_submits

                )
            if submitted >= max_submits:
                break
        print ""
        if not submitted and len(tracker.running) == 0:
            # Nothing left to submit and all jobs have finished running
            # Note: A kill command sent to the parent still wont kill the children processes
            done = True
        else:
            time.sleep(delay)
    print "Done submitting jobs!"
    print "IMPORTANT: Make sure to check the condor_q for any held jobs!"
    #print "IMPORTANT: There could still be (soon to be orphaned) running jobs, make sure to check that they complete properly!"

# Creates 1-D gridpacks at multiple linspaced starting points for each WC specified
def submit_1dim_jobs(gp,dofs,npts,runs,tag_postfix='',max_submits=-1,run_wl={}):
    submitted = 0
    delay    =  10.0   # Time between successful submits (in seconds)
    def_low  = -20.0
    def_high =  20.0
    wc_limits = parse_limit_file(os.path.join("addons/limits","dim6top_LO_UFO_limits.txt"))
    for dof in dofs:
        dof_subset = [dof]
        dof_name = dof.getName()
        lim_key = gp.ops['process'] + '_' + dof_name
        tag = dof_name + tag_postfix
        if wc_limits.has_key(lim_key):
            low_lim  = round(wc_limits[lim_key][0],6)
            high_lim = round(wc_limits[lim_key][1],6)
        else:
            low_lim  = def_low
            high_lim = def_high
        for idx,start in enumerate(linspace(low_lim,high_lim,runs)):
            if run_wl.has_key(dof_name) and idx not in run_wl[dof_name]:
                continue
            pt = {}
            for dof in dof_subset:
                pt[dof.getName()] = start
                dof.setLimits(start,low_lim,high_lim)  # Manually set the limits
            gp.configure(
                tag=tag,
                run=idx,
                dofs=dof_subset,
                num_pts=npts,
                start_pt=pt
            )
            if not gp.exists():
                gp.setup()
                print gp.baseSettings(),
                submitted += gp.submit()
                time.sleep(delay)
                print ""
            else:
                print "Skipping gridpack: %s" % (gp.getSetupString())
            if max_submits > 0 and submitted >= max_submits:
                return submitted
    return submitted

# Creates n-D gridpacks using as many starting points as possible
#   Note: If more runs are requested then available starting points, the gridpack
#         will automatically choose a random starting point
def submit_ndim_jobs(gp,dofs,npts,runs,tag,start_pts=[],max_submits=-1):
    submitted = 0
    delay = 10.0   # Time between successful submits (in seconds)
    for idx in range(runs):
        for dof in dofs:
            # Make sure the dofs have no limits set
            dof.setLimits(0,None,None)
        pt = {}
        if idx < len(start_pts):
            for k,v in start_pts[idx].iteritems(): pt[k] = v
        gp.configure(
            tag=tag,
            run=idx,
            dofs=dofs,
            num_pts=npts,
            start_pt=pt,
            def_limits=[-20.0,20.0]
        )
        if not gp.exists():
            gp.setup()
            print gp.baseSettings(),
            submitted += gp.submit()
            time.sleep(delay)
            print ""
        else:
            print "Skipping gridpack: %s" % (gp.getSetupString())
        if max_submits > 0 and submitted >= max_submits:
            return submitted
    return submitted

# Creates gridpacks using starting points and rwgt points extracted from scanpoints files
def submit_scanfile_jobs(gp,dofs,tag,scan_files,max_submits=-1):
    submitted = 0
    delay = 10.0
    for idx,file in enumerate(scan_files):
        if not os.path.exists(file):
            continue
        gp.configure(
            tag=tag,
            run=idx,
            dofs=dofs,
            num_pts=0,
            scan_file=file
        )
        if not gp.exists():
            gp.setup()
            print gp.baseSettings(),
            submitted += gp.submit()
            time.sleep(delay)
            print ""
        else:
            print "Skipping gridpack: %s" % (gp.getSetupString())
        if max_submits > 0 and submitted >= max_submits:
            return submitted
    return submitted

def main():
    random.seed()
    stype = ScanType.NONE
    btype = BatchType.NONE
    tag   = 'ExampleTag'
    runs  = 7               # if set to 0, will only make a single gridpack
    npts  = 10
    scan_files = [
        'scanfiles/ttll_16DOldLimitsAxisScan_run0_scanpoints.txt',
        'scanfiles/ttll_16DOldLimitsAxisScan_run1_scanpoints.txt',
        'scanfiles/ttll_16DOldLimitsAxisScan_run2_scanpoints.txt',
    ]
    proc_list = [ttH]
    dof_list  = [
        ctp,cpQM,ctW,ctZ,ctG,cbW,cpQ3,cptb,cpt,
        cQl3i,cQlMi,cQei,ctli,ctei,ctlSi,ctlTi
    ]

    proc_run_wl = {}    # {proc_name: {dof_name: [runs] } }
    start_pt = {}       # {wc_name: val}
    sm_pt    = {}
    for dof in dof_list: sm_pt[dof.getName()] = 0.0

    if stype == ScanType.SLINSPACE:
        tag = tag + "AxisScan"
    elif stype == ScanType.FRANDOM:
        tag = tag + "FullScan"

    if btype == BatchType.CMSCONNECT:
        # For submitting on CMSCONNECT, uses a way to track running jobs
        cmsconnect_chain_submit(
            dofs=dof_list,
            proc_list=proc_list,
            tag_postfix=tag,
            rwgt_pts=npts,
            runs=runs,
            stype=stype,
            scan_files=scan_files,
            proc_run_wl=proc_run_wl)
        return

    # Generic gridpack production example
    submitted = 0
    for p in proc_list:
        gridpack = Gridpack(
            process=p,
            stype=stype,
            btype=btype
        )

        # For creating feynman diagrams
        #gridpack.ops['save_diagrams'] = True
        #gridpack.ops['use_coupling_model'] = True
        #gridpack.ops['coupling_string'] = "FCNC=0 DIM6^2==1 DIM6_ctZ^2==1 DIM6_ctW^2==1"

        # If runs == 0, we probably are trying to make specific types of gridpacks by hand
        if stype == ScanType.FRANDOM and runs:
            submitted += submit_ndim_jobs(
                gp=gridpack,
                dofs=dof_list,
                npts=npts,
                runs=runs,
                tag=tag,
                start_pts=[],
                max_submits=-1
            )
        elif stype == ScanType.SLINSPACE and runs:
            submitted += submit_1dim_jobs(
                gp=gridpack,
                dofs=dof_list,
                npts=npts,
                runs=runs,
                tag_postfix=tag,
                max_submits=-1,
                run_wl={}
            )
        elif stype == ScanType.FROMFILE and runs:
            submitted += submit_scanfile_jobs(
                gp=gridpack,
                dofs=dof_list,
                tag=tag,
                scan_files=scan_files,
                max_submits=-1
            )
        else:
            gridpack.configure(tag=tag,run=0,dofs=dof_list,num_pts=npts,start_pt=start_pt)
            if not gridpack.exists():
                gridpack.setup()
                print gridpack.baseSettings(),
                submitted += gridpack.submit()
                print ""
            else:
                print "Skipping gridpack: %s" % (gridpack.getSetupString())

if __name__ == "__main__":
    main()
    print "\nFinished!"