import os
import datetime
import math
from helper_tools import run_process

# Utility class for keeping track of gridpack production jobs
# NOTE: This assumes that all the relevant log files are in the same directory
class JobTracker(object):
    RUNNING = 'running'
    CODEGEN = 'codegen'
    INTEGRATE = 'intg_all'
    INTEGRATE_FILTER = 'intg_filter'
    STUCK = 'stuck'
    FINISHED = 'finished'

    @classmethod
    def getJobTypes(cls):
        return [cls.RUNNING,cls.CODEGEN,cls.INTEGRATE,cls.FINISHED]

    @classmethod
    def formatTime(cls,t):
        h = max(math.floor(t/3600.0),0)
        m = max(math.floor((t - h*3600)/60.0),0)
        s = max(t - h*3600 - m*60,0)

        h = "%d" % (h)
        m = "%d" % (m)
        s = "%d" % (s)
        return (h,m,s)
        #return (h.rjust(2,"0"),m.rjust(2,"0"),s.rjust(2,"0"))

    def __init__(self,fdir='.'):
        self.fdir = fdir        # Where to look for output files
        self.intg_cutoff = -1
        self.stuck_cutoff = -1
        self.update()

    def update(self):
        self.last_update = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.all = self.getJobs()
        self.running = self.getRunningJobs()
        self.codegen = self.getCodeGenJobs()
        self.intg_full = self.getIntegrateJobs()
        self.intg_filter = self.getIntegrateJobs(self.intg_cutoff)
        self.stuck = self.getStuckJobs(self.stuck_cutoff)
        self.finished = self.getFinishedJobs()

    def setIntegrateCutoff(self,v):
        self.intg_cutoff = v

    def setStuckCutoff(self,v):
        self.stuck_cutoff = v

    # Return a list of scanpoint files in the target directory
    def getScanpointFiles(self,fdir='.'):
        fnames = []
        for fn in os.listdir(fdir):
            fpath = os.path.join(fdir,fn)
            if os.path.isdir(fpath):
                continue
            if fn.find("_scanpoints.txt") < 0:
                continue
            fnames.append(fn)
        return fnames

    # Check if the job has produced a tarball
    def hasTarball(self,chk_file,fdir='.'):
        arr = chk_file.split('_')
        p,c,r = arr[:3]
        fpath = os.path.join(fdir,"%s_%s_%s_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz" % (p,c,r))
        return os.path.exists(fpath)

    # Check if the job is still in the code gen phase
    def isCodeGen(self,chk_file,fdir='.'):
        arr = chk_file.split('_')
        p,c,r = arr[:3]
        log_fpath = os.path.join(fdir,"%s_%s_%s.log" % (p,c,r))
        input_fpath = os.path.join(fdir,"input_%s_%s_%s.tar.gz" % (p,c,r))
        codegen1_fpath = os.path.join(fdir,"codegen_%s_%s_%s.sh" % (p,c,r))
        codegen2_fpath = os.path.join(fdir,"codegen_%s_%s_%s.jdl" % (p,c,r))
        if not os.path.exists(log_fpath):
            return True
        elif os.path.exists(input_fpath):
            return True
        elif os.path.exists(codegen1_fpath):
            return True
        elif os.path.exists(codegen2_fpath):
            return True
        else:
            return False

    def isStuck(self,fn):
        return (fn in self.stuck)

    def isJob(self,file):
        return (file in self.all)

    # Returns a list of all jobs (based on scanpoints.txt file)
    def getJobs(self):
        sp_files = self.getScanpointFiles(self.fdir)
        jobs = []
        for fn in sp_files:
            arr = fn.split('_')
            jobs.append('_'.join(arr[:3]))
        return jobs

    # Returns a list of all jobs which have produced a tarball
    def getFinishedJobs(self):
        jobs = self.getJobs()
        finished = []
        for fn in jobs:
            if self.hasTarball(fn,self.fdir):
                finished.append(fn)
        return finished

    # Returns a list of all jobs which have not yet produced a tarball
    def getRunningJobs(self):
        jobs = self.getJobs()
        running = []
        for fn in jobs:
            if self.hasTarball(fn,self.fdir):
                continue
            running.append(fn)
        return running

    # Returns the subset of running jobs which are in the codegen phase
    def getCodeGenJobs(self):
        running = self.getRunningJobs()
        subset  = []
        for fn in running:
            if self.isCodeGen(fn,self.fdir):
                subset.append(fn)
        return subset

    # Returns the subset of running jobs which are in the integrate phase
    def getIntegrateJobs(self,cutoff=-1):
        running = self.getRunningJobs()
        subset = []
        for fn in running:
            if self.isCodeGen(fn,self.fdir):
                continue
            t = self.getIntegrateTime(fn)
            if cutoff > -1 and t > cutoff:
                continue
            subset.append(fn)
        return subset

    # Checks for stuck jobs (Note: This only checks the .log file so won't catch jobs stuck in the CODEGEN phase)
    def getStuckJobs(self,cutoff):
        subset = []
        if cutoff < 0:
            return subset
        running = self.getRunningJobs()
        for fn in running:
            t = self.getStuckTime(fn)
            if t > cutoff:
                subset.append(fn)
        return subset

    # Returns how long the job has been in the integrate phase
    def getIntegrateTime(self,fn):
        if not self.isJob(fn):
            return 0
        p,c,r = fn.split('_')
        fpath1 = os.path.join(self.fdir,"%s_%s_%s_codegen.log" % (p,c,r))
        fpath2 = os.path.join(self.fdir,"%s_%s_%s.log" % (p,c,r))
        return self.getModifiedTimeDifference(fpath2,fpath1)

    # Returns time since the log file was last updated (relative to now)
    def getStuckTime(self,fn):
        if not self.isJob(fn):
            return 0
        p,c,r = fn.split('_')
        fpath = os.path.join(self.fdir,"%s_%s_%s.log" % (p,c,r))
        return self.getLastModifiedTime(fpath)

    # Returns the time the job spent in the codegen phase
    def getCodegenTime(self,fn):
        dt = 0
        if not self.isJob(fn):
            return dt
        p,c,r = fn.split('_')
        if fn in self.codegen:
            # The Job is still in the codegen phase --> use scanpoints file to determine time
            fpath = os.path.join(self.fdir,"%s_%s_%s_scanpoints.txt" % (p,c,r))
            dt = self.getLastModifiedTime(fpath)
        else:
            # The job is out of the codegen phase
            fpath1 = os.path.join(self.fdir,"%s_%s_%s_scanpoints.txt" % (p,c,r))
            fpath2 = os.path.join(self.fdir,"%s_%s_%s_codegen.log" % (p,c,r))
            dt = self.getModifiedTimeDifference(fpath2,fpath1)
        return dt

    # Returns the absolute time difference (in seconds) since last modification between two files
    def getModifiedTimeDifference(self,fpath1,fpath2):
        if not os.path.exists(fpath1) or not os.path.exists(fpath2):
            return 0
        fstats1 = os.stat(fpath1)
        fstats2 = os.stat(fpath2)
        return int(abs(fstats2.st_mtime - fstats1.st_mtime))

    # Returns the time (relative to now) since the file was last modified
    def getLastModifiedTime(self,fpath):
        if not os.path.exists(fpath):
            return 0
        fstat = os.stat(fpath)
        tstamp = datetime.datetime.fromtimestamp(fstat.st_mtime)
        dt = datetime.datetime.now() - tstamp
        return (dt.days*3600*24 + dt.seconds)

    # Reads the last n lines from each of the jobs still in the integrate phase
    def checkProgress(self,lines=5):
        for fn in sorted(self.intg_full,key=self.getIntegrateTime):
            log_file = os.path.join(self.fdir,"%s.log" % (fn))
            if not os.path.exists(log_file):
                continue
            t = self.getIntegrateTime(fn)
            h,m,s = self.formatTime(t)
            int_tstr = "[%s:%s:%s]" % (h.rjust(2,"0"),m.rjust(2,"0"),s.rjust(2,"0"))
            t = self.getLastModifiedTime(log_file)
            h,m,s = self.formatTime(t)
            mod_tstr = "[%s:%s:%s]" % (h.rjust(2,"0"),m.rjust(2,"0"),s.rjust(2,"0"))
            print "\nChecking: %s - %s - %s" % (fn,int_tstr,mod_tstr)
            run_process(['tail','-n%d' % (lines),log_file])

    def displayJobList(self,s,arr):
        print "%s Jobs: %d" % (s,len(arr))
        for f in arr:
            print "\t%s" % (f)

    def showJobs(self,wl=[]):
        print "Last Update: %s" % (self.last_update)
        if len(wl) == 0:
            wl = self.getJobTypes()

        if self.RUNNING in wl:
            self.displayJobList("Running",  self.running)
        if self.CODEGEN in wl:
            self.displayJobList("CodeGen",  self.codegen)
        if self.INTEGRATE in wl:
            self.displayJobList("Integrate",self.intg_full)
        if self.INTEGRATE_FILTER in wl:
            self.displayJobList("Integrate(f)",self.intg_filter)
        if self.STUCK in wl:
            self.displayJobList("Stuck",self.stuck)
        if self.FINISHED in wl:
            self.displayJobList("Finished", self.finished)

if __name__ == "__main__":
    curr_dir = os.getcwd()
    tracker = JobTracker(fdir=curr_dir)
    t_cutoff = 30*60
    s_cutoff = 30*60
    tracker.setIntegrateCutoff(t_cutoff)
    tracker.setStuckCutoff(s_cutoff)
    tracker.update()
    job_list = [JobTracker.RUNNING,JobTracker.CODEGEN,JobTracker.INTEGRATE,JobTracker.INTEGRATE_FILTER,JobTracker.STUCK]
    tracker.showJobs(wl=job_list)
    tracker.checkProgress(lines=5)