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