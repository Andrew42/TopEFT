import os
import subprocess
import shutil
import random

from BatchType import BatchType
from ScanType import ScanType
from DegreeOfFreedom import DegreeOfFreedom
from helper_tools import *

# Class for configuring and setting up the submission for a single gridpack, can also run a produced gridpack tarball
class Gridpack(object):
    def __init__(self,process,limits_name,proc_card,template_dir,stype=ScanType.NONE,btype=BatchType.NONE):
        self.HOME_DIR      = os.getcwd()
        self.CARD_DIR      = os.path.join("addons","cards")
        self.LIMITS_DIR    = os.path.join("addons","limits")
        self.PROC_CARD_DIR = os.path.join("addons","cards","process_cards")
        self.GRIDRUN_DIR   = 'gridruns'

        # MadGraph specific card naming
        self.MG_PROC_CARD     = 'proc_card.dat'
        self.MG_CUSTOM_CARD   = 'customizecards.dat'
        self.MG_REWEIGHT_CARD = 'reweight_card.dat'
        self.MG_RUN_CARD      = 'run_card.dat'

        # The custom limits file for determining range of WC values
        self.LIMITS_FILE = "dim6top_LO_UFO_limits.txt"

        # These file name conventions are determined by genproductions scripts
        self.TARBALL_POSTFIX  = 'tarball'
        self.TARBALL_TYPE     = 'tar.xz'

        # These file name conventions have been freely chosen by us
        #   Note: Even though they were chosen arbitrarily, they are still used by code after
        #         the gridpack generation stage
        self.SCANFILE_POSTFIX = 'scanpoints'
        self.SCANFILE_TYPE    = 'txt'

        # Used when naming the final gridpack tarball
        self.CURR_ARCH        = 'slc6_amd64_gcc630'
        self.CURR_RELEASE     = 'CMSSW_9_3_0'

        # The script that is used to actually run the gridpack production
        self.GENPROD_SCRIPT   = 'gridpack_generation.sh'

        self.SAVE_DIAGRAMS      = False  # Need to modify generate_gridpack.sh if set to true (else gets cleaned up)
        self.USE_COUPLING_MODEL = False  # Replace the default dim6 model with the 'coupling_orders' version
        self.COUPLING_STRING    = "DIM6=0" # Only used if self.use_coupling_model is set to true

        self.ops = {
            'btype': btype,
            'stype': stype,
            'process': process,
            'tag': 'Test',
            'run': 0,
            'coeffs': [],
            'start_pt': {},
            'num_rwgt_pts': 0,
            'limits_name': limits_name,     # The process name as it appears in the limits file
            'process_card': proc_card,      # The name of the process card to be used (e.g. ttHDecay.dat)
            'template_dir': template_dir,   # The path (relative to the CARD_DIR) to the dir with the template run and customize cards
        }

        self.scan_pts = []
        self.is_configured = False
        return

    ################################################################################################
    def getSetupString(self):
        """ Construct the gridpack setup string (basically the name of the gridpack) """
        return "%s_%s_run%d" % (self.ops['process'],self.ops['tag'],self.ops['run'])

    def getTarballString(self):
        """ Construct the tarball file string """
        setup = self.getSetupString()
        return '%s_%s_%s_%s.%s' % (setup,self.CURR_ARCH,self.CURR_RELEASE,self.TARBALL_POSTFIX,self.TARBALL_TYPE)

    def getScanfileString(self):
        """ Construct the scanpoints file string """
        setup = self.getSetupString()
        return '%s_%s.%s' % (setup,self.SCANFILE_POSTFIX,self.SCANFILE_TYPE)

    def getGridrunOutputDirectory(self,create=False):
        """
            Returns the full path to the directory were unpacking and running of a generated gridpack
            will take place
        """
        if os.getcwd() != self.HOME_DIR:
            err_str = "Call to getGridrunOutputDirectory() when not in self.HOME_DIR!"
            err_str += "\n\tself.HOME_DIR: %s" % (self.HOME_DIR)
            err_str += "\n\tos.getcwd():   %s" % (os.getcwd())
            raise RuntimeError(err_str)
        if create and not os.path.exists(self.GRIDRUN_DIR):
            os.mkdir(self.GRIDRUN_DIR)
        process_subdir = os.path.join(self.GRIDRUN_DIR,self.ops['process'])
        if create and not os.path.exists(process_subdir):
            os.mkdir(process_subdir)
        setup = self.getSetupString()
        return os.path.join(process_subdir,"%s" % (setup))

    def getTargetDirectory(self,create=False):
        """
            Returns the full path to the directory were the genproductions framework will in order
            to read the MadGraph cards
        """
        if os.getcwd() != self.HOME_DIR:
            err_str = "Call to getTargetDirectory() when not in self.HOME_DIR!"
            err_str += "\n\tself.HOME_DIR: %s" % (self.HOME_DIR)
            err_str += "\n\tos.getcwd():   %s" % (os.getcwd())
            raise RuntimeError(err_str)
        process_subdir = os.path.join(self.CARD_DIR,"%s_cards" % (self.ops['process']))
        if create and not os.path.exists(process_subdir):
            os.mkdir(process_subdir)
        setup = self.getSetupString()
        target_dir = os.path.join(process_subdir,setup)
        if create and not os.path.exists(target_dir):
            os.mkdir(target_dir)
        return target_dir

    def exists(self):
        """ 
            Checks for the existence of certain files/directories in order to determine if this
            gridpack configuration has already been produced (or is in the process of being produced)
        """
        if os.getcwd() != self.HOME_DIR:
            err_str  = "Call to exists() when not in self.HOME_DIR!"
            err_str += "\n\tself.HOME_DIR: %s" % (self.HOME_DIR)
            err_str += "\n\tos.getcwd():   %s" % (os.getcwd())
            raise RuntimeError(err_str)
        has_setup_dir = os.path.exists(self.getSetupString())
        has_tarball   = os.path.exists(self.getTarballString())
        has_scanfile  = os.path.exists(self.getScanfileString())
        has_gridrun   = os.path.exists(self.getGridrunOutputDirectory())
        return (has_setup_dir or has_tarball or has_gridrun or has_scanfile)

    def limitSettings(self,header=True,depth=0):
        """ Print settings related to the limits used for setting values of the DoFs """
        indent = "\t"*depth
        info = ""
        if header: info += indent + "Limit Settings: %s\n" % (self.getSetupString())
        for c,dof in self.ops['coeffs'].iteritems():
            key = "%s_%s" % (self.ops['limits_name'],c)
            if header: info += "\t"
            info += indent + "%s: [" % (key.ljust(11))
            for idx,v in enumerate(dof.limits):
                fstr = "%.2f" % (v)
                if idx:
                    info += ","
                info += "%s" % (fstr.rjust(1+3+3)) # sign + 3 digits + 2 decimals + decimal symbol
            info += "]\n"
        return info

    def directorySettings(self,header=True,depth=0):
        """ Print the settings for various directories to be used by the genproductions framework """
        indent = "\t"*depth
        info = ""
        if header: 
            info += indent + "Directory Settings: %s\n" % (self.getSetupString())
            indent += "\t"
        info += indent + "Home Dir    : %s\n" % (self.HOME_DIR)
        info += indent + "Target Dir  : %s\n" % (os.path.join(".",self.getTargetDirectory()))
        info += indent + "Template Dir: %s\n" % (os.path.join(".",os.path.join(self.CARD_DIR,self.ops['template_dir'])))
        info += indent + "Gridrun Dir : %s\n" % (os.path.join(".",self.getGridrunOutputDirectory()))
        return info

    def baseSettings(self,header=True,depth=0):
        """ Print a pruned down list of settings for this gridpack instance """
        indent = "\t"*depth
        info = ""
        if header: 
            info += indent + "Base Settings: %s\n" % (self.getSetupString())
            depth += 1
            indent += "\t"
        else:
            info += indent + "Setup: %s\n" % (self.getSetupString())
        info += indent + "Process     : %s\n" % (self.ops['process'])
        info += indent + "Process Card: %s\n" % (self.ops['process_card'])
        info += indent + "ScanType    : %s\n" % (self.ops['stype'])
        info += indent + "BatchType   : %s\n" % (self.ops['btype'])
        info += indent + "Rwgt Points : %d\n" % (self.ops['num_rwgt_pts'])
        info += indent + "Scan Points : %d\n" % (len(self.scan_pts))
        info += self.limitSettings(header=True,depth=depth)
        return info

    def configureSettings(self,header=True,depth=0):
        """ Print only the configure related settings for this gridpack instance """
        indent = "\t"*depth
        info = ""
        if header: 
            info += indent + "Configure gridpack: %s" % (self.getSetupString())
            indent += "\t"
        info += indent + ""

        info += "\n"
        return info

    def allSettings(self,header=True,depth=0):
        """ Print all settings for this gridpack instance """
        indent = "\t"*depth
        info = ""
        if header: 
            info += indent + "All Settings: %s\n" % (self.getSetupString())
            depth += 1
            indent += "\t"
        else:
            info += indent + "Setup: %s\n" % (self.getSetupString())
        info += indent + "Process     : %s\n" % (self.ops['process'])
        info += indent + "Limits Name : %s\n" % (self.ops['limits_name'])
        info += indent + "Process Card: %s\n" % (self.ops['process_card'])
        info += indent + "ScanType    : %s\n" % (self.ops['stype'])
        info += indent + "BatchType   : %s\n" % (self.ops['btype'])
        info += indent + "Tarball File: %s\n" % (self.getTarballString())
        info += indent + "Scan File   : %s\n" % (self.getScanfileString())
        info += indent + "Rwgt Points : %d\n" % (self.ops['num_rwgt_pts'])
        info += indent + "Scan Points : %d\n" % (len(self.scan_pts))
        info += self.directorySettings(header=False,depth=depth)
        info += self.limitSettings(header=True,depth=depth)
        return info

    ################################################################################################

    def configure(self,tag,run,dofs,num_pts,start_pt={},def_limits=[-10.0,10.0],scan_file=None):
        """
            Prases options to produce gridpack in a particular way.
        """
        if len(def_limits) != 2:
            print "Invalid input for default limits!"
            self.is_configured = False
            return

        self.ops['tag'] = tag
        self.ops['run'] = run
        self.ops['coeffs'] = {}
        for dof in dofs:    # Convert list of WCs to a dictionary
            self.ops['coeffs'][dof.getName()] = dof

        def_low  = def_limits[0]
        def_high = def_limits[1]

        if scan_file:
            # Set starting point and rwgt points based on a scanpoints file
            self.scan_pts = []
            pts = parse_scan_file(scan_file)
            missing_wc = []
            for idx,pt in enumerate(pts):
                new_pt = {}
                for k,v in pt.iteritems():
                    # Keep only the WCs which have been specified
                    if self.ops['coeffs'].has_key(k):
                        new_pt[k] = v
                if idx == 0:
                    # Set the starting point from the scanpoints file
                    for k in self.ops['coeffs'].keys():
                        if not new_pt.has_key(k):
                            # Any WCs which are missing from the scanpoints file are set to SM value
                            missing_wc.append(k)
                            self.ops['coeffs'][k].setLimits(0,0,0)
                        else:
                            self.ops['coeffs'][k].setLimits(new_pt[k],0,0)
                else:
                    self.scan_pts.append(new_pt)
            if len(missing_wc):
                print "[WARNING] Scanpoints file is missing WCs used in this gridpack configuration, %s" % (str(missing_wc))
            self.ops['num_rwgt_pts'] = len(self.scan_pts)
        else:
            self.scan_pts = []  # Clear the scan_pts array incase it was used previously

        if len(self.scan_pts) == 0:
            # The scan points will need to be set automatically
            if num_pts > 0:
                # Make sure we have enough points to reconstruct the parametrization
                if self.ops['stype'] == ScanType.FRANDOM:
                    N = len(self.ops['coeffs'].keys())
                    num_pts = max(num_pts,1.2*(1+2*N+N*(N-1)/2))
                elif self.ops['stype'] == ScanType.SLINSPACE:
                    num_pts = max(num_pts,3)
                num_pts = int(num_pts)
            self.ops['num_rwgt_pts'] = num_pts

            wc_limits = parse_limit_file(os.path.join(self.LIMITS_DIR,self.LIMITS_FILE))
            for idx,c in enumerate(self.ops['coeffs'].keys()):
                # Set the limits based on limits file (if needed/possible)
                if self.ops['coeffs'][c].hasLimits():
                    # The dof already has limits set
                    continue
                key = "%s_%s" % (self.ops['limits_name'],c)
                if wc_limits.has_key(key):
                    # Use limits based on those found in the limits file
                    low  = round(wc_limits[key][0],6)
                    high = round(wc_limits[key][1],6)
                else:
                    # The WC doesn't exist in the limits file, so use defaults
                    low  = def_low
                    high = def_high
                if start_pt.has_key(c):
                    strength = start_pt[c]
                else:
                    strength = calculate_start_point(low,high,1.25)
                self.ops['coeffs'][c].setLimits(strength,low,high)
        self.is_configured = True
        return

    def setup(self):
        """
            Sets up and/or creates the needed directories and files need for creating a gridpack
            using the genproductions framework
        """
        BatchType.isValid(self.ops['btype'])
        ScanType.isValid(self.ops['stype'])

        os.chdir(self.HOME_DIR)

        if self.exists():
            print "Skipping gridpack setup: %s" % (self.getSetupString())
            return False

        print "Setup gridpack: %s..." % (self.getSetupString())

        # Set the random seed adding extra events to the pilotrun
        seed = int(random.uniform(1,1e6))
        print "\tSeed: %d" % (seed)
        run_process(['sed','-i','-e',"s|RWSEED=[0-9]*|RWSEED=%d|g" % (seed),self.GENPROD_SCRIPT])

        target_dir = self.getTargetDirectory(create=False)
        if os.path.exists(target_dir):
            print "\tNOTE: The cards directory already exists, will overwrite existing cards."
        else:
            # Create the needed directories
            self.getTargetDirectory(create=True)

        setup = self.getSetupString()

        customize_src = os.path.join(self.HOME_DIR,self.CARD_DIR,self.ops['template_dir'],self.MG_CUSTOM_CARD)
        customize_tar = os.path.join(target_dir,"%s_%s" % (setup,self.MG_CUSTOM_CARD))
        shutil.copy(customize_src,customize_tar)

        run_src = os.path.join(self.HOME_DIR,self.CARD_DIR,self.ops['template_dir'],self.MG_RUN_CARD)
        run_tar = os.path.join(target_dir,"%s_%s" % (setup,self.MG_RUN_CARD))
        shutil.copy(run_src,run_tar)

        proc_src = os.path.join(self.HOME_DIR,self.PROC_CARD_DIR,self.ops['process_card'])
        proc_tar = os.path.join(target_dir,"%s_%s" % (setup,self.MG_PROC_CARD))
        shutil.copy(proc_src,proc_tar)

        # Sets the initial WC phase space point for MadGraph to start from (appends to customize card)
        set_initial_point(customize_tar,self.ops['coeffs'])

        scanfile = self.getScanfileString()
        rwgt_tar = os.path.join(target_dir,"%s_%s" % (setup,self.MG_REWEIGHT_CARD))
        
        if len(self.scan_pts) == 0:
            self.scan_pts = ScanType.getPoints(self.ops['coeffs'],self.ops['num_rwgt_pts'],self.ops['stype'])

        save_scan_points(scanfile,self.ops['coeffs'],self.scan_pts)
        make_reweight_card(rwgt_tar,self.ops['coeffs'],self.scan_pts)

        if self.SAVE_DIAGRAMS:
            # Remove the nojpeg option from the output line of the process card
            print "\tSaving diagrams!"
            run_process(['sed','-i','-e',"s|SUBSETUP -nojpeg|SUBSETUP|g",proc_tar])

        if self.USE_COUPLING_MODEL:
            print "\tUsing each_coupling_order model!"
            run_process(['sed','-i','-e',"s|DIM6=1|%s|g" % (self.COUPLING_STRING),proc_tar])

        # Replace SUBSETUP in the process card
        run_process(['sed','-i','-e',"s|SUBSETUP|%s|g" % (setup),proc_tar])
        return True

    def clean(self):
        """ Remove all folders/files created by the setup()/submit() methods """
        if not self.is_configured:
            print "The gridpack has not been configured yet, so no cleaning can be done!"
            return

        os.chdir(self.HOME_DIR)

        print "Cleaning files related to current gridpack configuration: %s" % (self.getSetupString())
        target_dir = self.getTargetDirectory(create=False)
        if os.path.exists(target_dir) and os.path.isdir(target_dir):
            # This is where the modfied madgraph cards are stored
            print "\tRemoving existing directory: %s " % (target_dir)
            shutil.rmtree(target_dir)

        setup_dir = self.getSetupString()
        if os.path.exists(setup_dir) and os.path.isdir(setup_dir):
            # This is the directory that gets created by the setup_production.sh script (in LOCAL or CMSCONNECT mode)
            print "\tRemoving existing directory: %s " % (setup_dir)
            shutil.rmtree(setup_dir)

        gridrun_dir = self.getGridrunOutputDirectory()
        if os.path.exists(gridrun_dir) and os.path.isdir(gridrun_dir):
            # This is the directory that is used to unpack and run a gridpack tarball
            print "\tRemoving existing directory: %s " % (gridrun_dir)
            shutil.rmtree(gridrun_dir)

        tarball_file = self.getTarballString()
        if os.path.exists(tarball_file) and not os.path.isdir(tarball_file):
            # This is the tarball created by the setup_production.sh script
            print "\tRemoving existing file: %s" % (tarball_file)
            os.remove(tarball_file)

        scanpoints_file = self.getScanfileString()
        if os.path.exists(scanpoints_file) and not os.path.isdir(scanpoints_file):
            # This is the txt file which contains the recorded starting point and list of madgraph rwgt points
            print "\tRemoving existing file: %s" % (scanpoints_file)
            os.remove(scanpoints_file)

        log_file = "%s.log" % (self.getSetupString())
        if os.path.exists(log_file) and not os.path.isdir(log_file):
            # This is the log file created by the setup_production.sh script
            print "\tRemoving existing file: %s" % (log_file)
            os.remove(log_file)

        debug_file = "%s.debug" % (self.getSetupString())
        if os.path.exists(debug_file) and not os.path.isdir(debug_file):
            # This is the debug file created by the setup_production.sh script (only in CMSCONNECT mode)
            print "\tRemoving existing file: %s" % (debug_file)
            os.remove(debug_file)

        codegen_file = "%s_codegen.log" % (self.getSetupString())
        if os.path.exists(codegen_file) and not os.path.isdir(codegen_file):
            # This is the codegen log file created by the setup_production.sh script (only in CMSCONNECT mode)
            print "\tRemoving existing file: %s" % (codegen_file)
            os.remove(codegen_file)

        self.is_configured = False

    def submit(self):
        """ Run one of genproductions gridpack generation scripts """
        setup = self.getSetupString()
        target_dir = self.getTargetDirectory()
        btype = self.ops['btype']
        if not os.path.exists(target_dir):
            print "[ERROR] Can't find target directory, %s" % (target_dir)
            return False
        print "Submit gridpack: %s..." % (setup)
        print "\tBatchType: %s" % (btype)
        if btype == BatchType.LOCAL:
            # For interactive/serial running
            run_process(['./gridpack_generation.sh',setup,target_dir])
            return True
        elif btype == BatchType.LSF:
            # For batch running
            run_process(['./submit_gridpack_generation.sh','15000','15000','1nd',setup,target_dir,'8nh'])
            return True
        elif btype == BatchType.CMSCONNECT:
            # For cmsconnect running
            debug_file = "%s.debug" % (setup)
            cmsconnect_cores = 1
            print '\tCurrent PATH: {0}'.format(os.getcwd())
            print '\tWill execute: ./submit_cmsconnect_gridpack_generation.sh {0} {1} {2} "{3}" {4} {5}'.format(setup,target_dir,str(cmsconnect_cores), "15 Gb",self.CURR_ARCH,self.CURR_RELEASE)
            subprocess.Popen(
                ["./submit_cmsconnect_gridpack_generation.sh",setup,target_dir,str(cmsconnect_cores),"15 Gb",self.CURR_ARCH,self.CURR_RELEASE],
                stdout=open(debug_file,'w'),
                stderr=subprocess.STDOUT
            )
            return True
        elif btype == BatchType.CONDOR:
            # Not currently working
            print "\tCondor running is not currently working. Sorry!"
            #run_process(['./submit_condor_gridpack_generation.sh',setup,target_dir])
            return True
        elif btype == BatchType.NONE:
            print "\tSkipping gridpack generation, %s" % (setup)
            return True
        return False

    def run(self,events,seed,cores):
        """ Unapack and run an existing gridpack to produce events in an LHE file """
        os.chdir(self.HOME_DIR)

        setup = self.getSetupString()
        print "Running Gridpack: %s" % (setup)
        print "\tSetting up directories..."

        output_dir = self.getGridrunOutputDirectory(create=True)
        if os.path.exists(output_dir):
            # We already ran the gridpack once!
            print "Output directory already exists, skipping gridpack run: %s" % (setup)
            return
        else:
            os.mkdir(output_dir)

        tarball = self.getTarballString()
        if not os.path.exists(tarball):
            print "No tarball file found! Skipping..."
            return
        print "\tMoving tarball..."
        shutil.move(tarball,output_dir)

        os.chdir(output_dir)

        print "\tExtracting tarball..."
        run_process(['tar','xaf',tarball])

        print "\tRunning gridpack..."
        run_process(['./runcmsgrid.sh',str(events),str(seed),str(cores)])

        os.chdir(self.HOME_DIR)
        return

if __name__ == "__main__":
    gridpack = Gridpack(
        process='ttllDecay',
        limits_name='ttll',
        proc_card='ttllDecay.dat',
        template_dir='template_cards/defaultPDFs_template',
        stype=ScanType.NONE,
        btype=BatchType.NONE
    )

    ctG  = DegreeOfFreedom(name='ctG'  ,relations=[['ctG'] ,1.0])
    ctW  = DegreeOfFreedom(name='ctW'  ,relations=[['ctW'] ,1.0])
    ctei = DegreeOfFreedom(name='ctei' ,relations=[['cte1','cte2','cte3'],1.0])

    dof_list = [ctG,ctW,ctei]

    ################################################################################################
    # Example using Gridpack class
    pt = {}
    for dof in dof_list:
        dof.setLimits(4.0,-50.0,50.0)   # Will force the code to use these limits instead of from the limits file
        pt[dof.getName()] = 4.0
    gridpack.configure(
        tag='ExampleTagName',
        run=0,
        dofs=dof_list,
        num_pts=10,
        start_pt=pt     # Optional, if not set will select starting strength for each WC randomly
    )
    if not gridpack.exists():
        result = gridpack.setup()
        result = gridpack.submit()
    ################################################################################################
    # Example generating multiple gridpacks at different 1-D starting points
    for dof in dof_list:
        tag = dof.getName() + 'ExampleTagName'
        dof_subset = [dof]
        for idx,v in enumerate(linspace(-50.0,50.0,5)):
            gridpack.configure(
                tag=tag,
                run=idx,
                dofs=dof_subset,
                num_pts=10
            )
            if not gridpack.exists():
                result = gridpack.setup()
                result = gridpack.submit()
    ################################################################################################
    # Example generating multiple gridpacks at random n-D starting points
    tag = 'ExampleTagName'
    for idx in range(5):
        pt = {}
        for dof in dof_list:
            pt[dof.getName()] = calculate_start_point(-50.0,50.0)
        gridpack.configure(
            tag=tag,
            run=idx,
            dofs=dof_list,
            num_pts=10,
            start_pt=pt
        )
        if not gridpack.exists():
            result = gridpack.setup()
            result = gridpack.submit()

