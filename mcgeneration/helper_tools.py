import itertools

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

# Checks if two W.C. phase space points are identical
def check_point(pt1,pt2):
    for k,v in pt1.iteritems():
        if not pt2.has_key(k):
            pt2[k] = 0.0    # pt2 is missing the coeff, add it and set it to SM value
        if v != pt2[k]:
            return False
    return True

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

    @classmethod
    def getPoints(cls,limits,num_pts,stype):
        pts = []
        if stype == cls.FLINSPACE:
            # Full scan of phase space using a linear grid spacing
            pts = cls.fullScanLinear(limits,num_pts)
        elif stype == cls.FRANDOM:
            # Full scan of phase space using random sampling of the entire phase space
            pts = cls.fullScanRandom(limits,num_pts)
        elif stype == cls.SLINSPACE:
            # Axis scan (no xterms) with linear spacing along the axis
            pts = cls.axisScanLinear(limits,num_pts)
        elif stype == cls.SRANDOM:
            # Axis scan (no xterms) with random sampling along the axis
            cls.axisScanRandom(limits,num_pts)
        elif stype == cls.NONE:
            pts = []
        return pts

    @classmethod
    def fullScanLinear(cls,limits,num_pts):
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

    @classmethod
    def fullScanRandom(cls,limits,num_pts):
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

    @classmethod
    def axisScanLinear(cls,limits,num_pts):
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

    @classmethod
    def axisScanRandom(cls,limits,num_pts):
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

# Currently in testing
class DegreeOfFreedom(object):
    def __init__(self,name,wc_relations={}):
        self.name = name
        self.relations = wc_relations

    def getName(self):
        return self.name

    def getCoefficients(self):
        return self.relations.keys()

    def setCoefficient(self,wc_names,scale):
        for wc in wc_names:
            self.relations[wc] = scale

    def removeCoefficient(self,wc_name):
        return self.relations.pop(wc_name,None)

    def eval(self,x):
        output = {}
        for wc,scale in self.relations.iteritems():
            output[wc] = x*scale
        return output


# cQDW = 1.0*cQq13 = 1.0*cQl3(l)
# cQDB = 6.0*cQq11 = 1.5*cQu1 = -3.0*cQd1 = -3.0*cQb1 = -2.0*cQlM(l) = -1.0*cQe(l)
# ctDB = 6.0*ctq1  = 1.5*ctu1 = -3.0*ctd1 = -3.0*ctb1 = -2.0*ctl(l)  = -1.0*cte(l)
# cQDG = 1.0*cQq81 = 1.0*cQu8 =  1.0*cQd8 =  1.0*cQb8  **NOT SURE ABOUT cQq81**
# ctDG = 1.0*ctq8  = 1.0*ctu8 =  1.0*ctd8 =  1.0*ctb8

#ANALYSIS_COEFFS = [ # As suggested by Adam
#    'ctp','cpQM','cpQ3','cpt','cptb','ctW', 'ctZ', 'cbW','ctG',
#    'cQl31','cQlM1','cQe1','ctl1','cte1','ctlS1','ctlT1',
#    'cQl32','cQlM2','cQe2','ctl2','cte2','ctlS2','ctlT2',
#    'cQl33','cQlM3','cQe3','ctl3','cte3','ctlS3','ctlT3',
#]

#TOP_PHILIC_COEFFS = [
#    'ctp','cpQM','cpQ3','cpt','cptb','ctW', 'ctZ', 'cbW','ctG', # 9 two-heavy quark + bosons
#    'cQQ1','cQQ8','cQt1','cQt8','ctt1',                         # 5 four heavy quarks
#]

if __name__ == "__main__":
    ctp  = DegreeOfFreedom('ctp',{'ctp',1.0})
    cpQM = DegreeOfFreedom('cpQM',{'cpQM',1.0})
    cpQ3 = DegreeOfFreedom('cpQ3',{'cpQ3',1.0})
    cpt  = DegreeOfFreedom('cpt',{'cpt',1.0})
    cptb = DegreeOfFreedom('cptb',{'cptb',1.0})
    ctW  = DegreeOfFreedom('ctW',{'ctW',1.0})
    ctZ  = DegreeOfFreedom('ctZ',{'ctZ',1.0})
    cbW  = DegreeOfFreedom('cbW',{'cbW',1.0})
    ctG  = DegreeOfFreedom('ctG',{'ctG',1.0})
    cQQ1 = DegreeOfFreedom('cQQ1',{'cQQ1',1.0})
    cQQ8 = DegreeOfFreedom('cQQ8',{'cQQ8',1.0})
    cQt1 = DegreeOfFreedom('cQt1',{'cQt1',1.0})
    cQt8 = DegreeOfFreedom('cQt8',{'cQt8',1.0})
    ctt1 = DegreeOfFreedom('ctt1',{'ctt1',1.0})

    cQDW = DegreeOfFreedom(name='cQDW')
    cQDW.setCoefficient(['cQq13','cQl31','cQl32','cQl33'],1.0)

    cQDB = DegreeOfFreedom(name='cQDB')
    cQDB.setCoefficient(['cQq11'],6.0)
    cQDB.setCoefficient(['cQu1'],1.5)
    cQDB.setCoefficient(['ctd1','ctb1'],-3.0)
    cQDB.setCoefficient(['cQlM1','cQlM2','cQlM3'],-2.0)
    cQDB.setCoefficient(['cQe1','cQe2','cQe3'],-1.0)

    ctDB = DegreeOfFreedom(name='ctDB')
    ctDB.setCoefficient(['ctq1'],6.0)
    ctDB.setCoefficient(['ctu1'],1.5)
    ctDB.setCoefficient(['ctd1','ctb1'],-3.0)
    ctDB.setCoefficient(['ctl1','ctl2','ctl3'],-2.0)
    ctDB.setCoefficient(['cte1','cte2','cte3'],-1.0)

    cQDG = DegreeOfFreedom(name='cQDG')
    cQDG.setCoefficient(['cQq81','cQu8','cQd8','cQb8'],1.0)

    ctDG = DegreeOfFreedom(name='ctDG')
    ctDG.setCoefficient(['ctq8','ctu8','ctd8','ctb8'],1.0)