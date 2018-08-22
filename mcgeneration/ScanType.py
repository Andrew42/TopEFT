from helper_tools import *

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
    def getPoints(cls,dofs,num_pts,stype):
        pts = []
        if stype == cls.FLINSPACE:
            # Full scan of phase space using a linear grid spacing
            pts = cls.fullScanLinear(dofs,num_pts)
        elif stype == cls.FRANDOM:
            # Full scan of phase space using random sampling of the entire phase space
            pts = cls.fullScanRandom(dofs,num_pts)
        elif stype == cls.SLINSPACE:
            # Axis scan (no xterms) with linear spacing along the axis
            pts = cls.axisScanLinear(dofs,num_pts)
        elif stype == cls.SRANDOM:
            # Axis scan (no xterms) with random sampling along the axis
            cls.axisScanRandom(dofs,num_pts)
        elif stype == cls.NONE:
            pts = []
        return pts

    @classmethod
    def fullScanLinear(cls,dofs,num_pts):
        pts_arr = []
        if num_pts == 0:
            return pts_arr
        sm_pt = {}
        start_pt = {}
        coeffs = dofs.keys()
        arr = []
        for c in coeffs:
            sm_pt[c] = 0.0
            start_pt[c] = dofs[c].getStart()
            arr += [linspace(dofs[c].getLow(),dofs[c].getHigh(),num_pts)]
        has_sm_pt = check_point(sm_pt,start_pt)
        mesh_pts = [a for a in itertools.product(*arr)]
        for rwgt_pt in mesh_pts:
            pt = {}
            for idx,c in enumerate(coeffs):
                pt[c] = round(rwgt_pt[idx],6)
            if check_point(pt,sm_pt):
                # Skip SM point
                has_sm_pt = True
            if check_point(pt,start_pt):
                # Skip starting point
                continue
            pts_arr.append(pt)
        if not has_sm_pt:
            pts_arr.append(sm_pt)
        return pts_arr

    @classmethod
    def fullScanRandom(cls,dofs,num_pts):
        pts_arr = []
        if num_pts == 0:
            return pts_arr
        sm_pt = {}
        start_pt = {}
        coeffs = dofs.keys()
        for c in coeffs:
            sm_pt[c] = 0.0
            start_pt[c] = dofs[c].getStart()
        has_sm_pt = check_point(sm_pt,start_pt)
        for idx in range(num_pts):
            pt = {}
            for c in coeffs:
                pt[c] = round(random.uniform(dofs[c].getLow(),dofs[c].getHigh()),6)
            if check_point(pt,sm_pt):
                has_sm_pt = True
            if check_point(pt,start_pt):
                continue
            pts_arr.append(pt)
        if not has_sm_pt:
            pts_arr.append(sm_pt)
        return pts_arr

    @classmethod
    def axisScanLinear(cls,dofs,num_pts):
        pts_arr = []
        if num_pts == 0:
            return pts_arr
        sm_pt = {}
        start_pt = {}
        coeffs = dofs.keys()
        for c in coeffs:
            sm_pt[c] = 0.0
            start_pt[c] = dofs[c].getStart()
        has_sm_pt = check_point(sm_pt,start_pt)
        for c1 in coeffs:
            arr = linspace(dofs[c1].getLow(),dofs[c1].getHigh(),num_pts)
            for v in arr:
                pt = {}
                for c2 in coeffs:
                    if c1 == c2:
                        pt[c2] = round(v,6)
                    else:
                        pt[c2] = 0.0
                if check_point(pt,sm_pt):
                    has_sm_pt = True
                if check_point(pt,start_pt):
                    continue
                pts_arr.append(pt)
        if not has_sm_pt:
            pts_arr.append(sm_pt)
        return pts_arr

    @classmethod
    def axisScanRandom(cls,dofs,num_pts):
        pts_arr = []
        if num_pts == 0:
            return pts_arr
        sm_pt = {}
        start_pt = {}
        coeffs = dofs.keys()
        for c in coeffs:
            sm_pt[c] = 0.0
            start_pt[c] = dofs[c].getStart()
        has_sm_pt = check_point(sm_pt,start_pt)
        for c1 in coeffs:
            for idx in range(num_pts):
                pt = {}
                v = random.uniform(dofs[c1].getLow(),dofs[c1].getHigh())
                for c2 in coeffs:
                    if c1 == c2:
                        pt[c2] = round(v,6)
                    else:
                        pt[c2] = 0.0
                if check_point(pt,sm_pt):
                    has_sm_pt = True
                if check_point(pt,start_pt):
                    continue
                pts_arr.append(pt)
        if not has_sm_pt:
            pts_arr.append(sm_pt)
        return pts_arr

    @classmethod
    def axisScanLinearOLD(cls,limits,num_pts):
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
    def axisScanRandomOLD(cls,limits,num_pts):
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