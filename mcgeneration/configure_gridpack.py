import os
import subprocess
import shutil
import itertools
import random
import time

from helpers.helper_tools import linspace
from helpers.ScanType import ScanType
from helpers.BatchType import BatchType
from helpers.DegreeOfFreedom import DegreeOfFreedom
from helpers.JobTracker import JobTracker
from helpers.Gridpack import Gridpack

#python configure_gridpack.py >& output.log &

#NOTE: The template directory should contain run_card.dat and customizecards.dat files
PROCESS_MAP = {
    'ttH': {
        'name': 'ttH',
        'process_card': 'ttH.dat',
        'template_dir': 'template_cards/defaultPDFs_template'
    },
    'ttll': {
        'name': 'ttll',
        'process_card': 'ttll.dat',
        'template_dir': 'template_cards/centralTTZ_template'
    },
    'ttlnu': {
        'name': 'ttlnu',
        'process_card': 'ttlnu.dat',
        'template_dir': 'template_cards/defaultPDFs_template'
    },
    'tllq': {
        'name': 'tllq',
        'process_card': 'tllq.dat',
        'template_dir': 'template_cards/defaultPDFs_template'
    },
    'ttHJet': {
        'name': 'ttH',
        'process_card': 'ttHJet.dat',
        'template_dir': 'template_cards/jets_template'
    },
    'ttHDecay': {
        'name': 'ttH',
        'process_card': 'ttHDecay.dat',
        'template_dir': 'template_cards/defaultPDFs_template'
    }
}

ctp   = DegreeOfFreedom(name='ctp'  ,relations=[['ctp'] ,1.0])
cpQM  = DegreeOfFreedom(name='cpQM' ,relations=[['cpQM'],1.0])
cpQ3  = DegreeOfFreedom(name='cpQ3' ,relations=[['cpQ3'],1.0])
cpt   = DegreeOfFreedom(name='cpt'  ,relations=[['cpt'] ,1.0])
cptb  = DegreeOfFreedom(name='cptb' ,relations=[['cptb'],1.0])
ctW   = DegreeOfFreedom(name='ctW'  ,relations=[['ctW'] ,1.0])
ctZ   = DegreeOfFreedom(name='ctZ'  ,relations=[['ctZ'] ,1.0])
cbW   = DegreeOfFreedom(name='cbW'  ,relations=[['cbW'] ,1.0])
ctG   = DegreeOfFreedom(name='ctG'  ,relations=[['ctG'] ,1.0])
cQQ1  = DegreeOfFreedom(name='cQQ1' ,relations=[['cQQ1'],1.0])
cQQ8  = DegreeOfFreedom(name='cQQ8' ,relations=[['cQQ8'],1.0])
cQt1  = DegreeOfFreedom(name='cQt1' ,relations=[['cQt1'],1.0])
cQt8  = DegreeOfFreedom(name='cQt8' ,relations=[['cQt8'],1.0])
ctt1  = DegreeOfFreedom(name='ctt1' ,relations=[['ctt1'],1.0])
cQei  = DegreeOfFreedom(name='cQei' ,relations=[['cQe1','cQe2','cQe3'],1.0])
ctli  = DegreeOfFreedom(name='ctli' ,relations=[['ctl1','ctl2','ctl3'],1.0])
ctei  = DegreeOfFreedom(name='ctei' ,relations=[['cte1','cte2','cte3'],1.0])
cQl3i = DegreeOfFreedom(name='cQl3i',relations=[['cQl31','cQl32','cQl33'],1.0])
cQlMi = DegreeOfFreedom(name='cQlMi',relations=[['cQlM1','cQlM2','cQlM3'],1.0])
ctlSi = DegreeOfFreedom(name='ctlSi',relations=[['ctlS1','ctlS2','ctlS3'],1.0])
ctlTi = DegreeOfFreedom(name='ctlTi',relations=[['ctlT1','ctlT2','ctlT3'],1.0])

all_coeffs = [ctp,cpQM,cpQ3,cpt,cptb,ctW,ctZ,cbW,ctG,cQQ1,cQQ8,cQt1,cQt8,ctt1,cQei,ctli,ctei,cQl3i,cQlMi,ctlSi,ctlTi]

# For submitting many gridpack jobs on cmsconnect
def cmsconnect_chain_submit(dofs,proc_list,tag_postfix,rwgt_pts,runs,stype,scan_files=[]):
    tracker = JobTracker(fdir=os.getcwd())
    max_gen = 5         # Max number of CODEGEN jobs to have running
    max_int = 5         # Max number of INTEGRATE jobs to have running
    max_run = 50        # Max number of total jobs running
    int_cut = 45*60     # Time (relative to INTEGRATE step) before additional jobs can get submitted
    delay = 5.0*60      # Time between checks
    tracker.setIntegrateCutoff(int_cut)
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
        if max_submits <= 0 or done:
            time.sleep(delay)
            continue
        submitted = 0
        for p in proc_list:
            if not PROCESS_MAP.has_key(p):
                print "Missing process in PROCESS_MAP: %s" % (p)
                continue
            gridpack = Gridpack(
                process=p,
                limits_name=PROCESS_MAP[p]['name'],
                proc_card=PROCESS_MAP[p]['process_card'],
                template_dir=PROCESS_MAP[p]['template_dir'],
                stype=stype,
                btype=BatchType.CMSCONNECT
            )
            #TODO: Might not want to split it up like this
            if stype == ScanType.SLINSPACE:
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
        if not submitted:
            # Nothing left to submit --> There could still be jobs running (which will be orphaned and complete on their own)
            done = True
    print "Done submitting jobs!"
    print "IMPORTANT: There could still be (soon to be orphaned) running jobs, make sure to check that they complete properly!"

# Creates 1-D gridpacks at multiple linspaced starting points for each WC specified
def submit_1dim_jobs(gp,dofs,npts,runs,tag_postfix='',max_submits=-1,run_wl=[]):
    submitted = 0
    delay    =  10.0   # Time between successful submits (in seconds)
    low_lim  = -25.0
    high_lim =  25.0
    for dof in dofs:
        dof_subset = [dof]
        tag = dof.getName() + tag_postfix
        for idx,start in enumerate(linspace(low_lim,high_lim,runs)):
            if len(run_wl) > 0 and idx not in run_wl:
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
                print gp.baseSettings(),
                gp.setup()
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
            pt = start_pts[idx]
        gp.configure(
            tag=tag,
            run=idx,
            dofs=dofs,
            num_pts=npts,
            start_pt=pt,
            def_limits=[-20.0,20.0]
        )
        if not gp.exists():
            print gp.baseSettings(),
            gp.setup()
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
            print gp.baseSettings(),
            gp.setup()
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
    stype = ScanType.SLINSPACE
    btype = BatchType.NONE
    tag   = 'PartialCentralTTZ'
    runs  = 1
    npts  = 10
    scan_files = [
        'scanfiles/ttll_16DOldLimitsAxisScan_run0_scanpoints.txt',
        'scanfiles/ttll_16DOldLimitsAxisScan_run1_scanpoints.txt',
        'scanfiles/ttll_16DOldLimitsAxisScan_run2_scanpoints.txt',
    ]
    proc_list = ['ttll']
    dof_list  = [
        ctW,ctp,cpQM,ctZ,ctG,cbW,cpQ3,cptb,cpt,
        cQl3i,cQlMi,cQei,ctli,ctei,ctlSi,ctlTi
    ]

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
            scan_files=scan_files)
        return

    # Generic gridpack production example
    submitted = 0
    for p in proc_list:
        if not PROCESS_MAP.has_key(p):
            print "Missing process in PROCESS_MAP: %s" % (p)
            continue
        gridpack = Gridpack(
            process=p,
            limits_name=PROCESS_MAP[p]['name'],
            proc_card=PROCESS_MAP[p]['process_card'],
            template_dir=PROCESS_MAP[p]['template_dir'],
            stype=stype,
            btype=btype
        )

        if stype == ScanType.FRANDOM:
            submitted += submit_ndim_jobs(
                gp=gridpack,
                dofs=dof_list,
                npts=npts,
                runs=runs,
                tag=tag,
                start_pts=[],
                max_submits=-1
            )
        elif stype == ScanType.SLINSPACE:
            submitted += submit_1dim_jobs(
                gp=gridpack,
                dofs=dof_list,
                npts=rwgt_pts,
                runs=runs,
                tag_postfix=tag,
                max_submits=-1,
                run_wl=[]
            )

if __name__ == "__main__":
    main()
    print "\nFinished!"