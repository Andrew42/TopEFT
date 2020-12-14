#!/bin/bash

### settings to modify 
# define pathes (can be kept as is)
EFTMCPATH=`pwd -P`
# path should end with genproductions 
GENPRODPATH=${EFTMCPATH}/../../genproductions
if [ "$(hostname)" == "login.uscms.org" ]; then
    # path for cmsconnect submit node
    GENPRODPATH="/local-scratch/${USER}/genproductions"
fi
### end of settings 

### check out official genproduction repo, currently branch 2.6 
if [ -d "$GENPRODPATH" ]; then
    echo "Directory ${GENPRODPATH} shouldn't exist at this point"
    echo " We are going to set it up"
    echo " Please remove it and start over again"
    exit 0
else 
    mkdir -p ${GENPRODPATH}
    cd ${GENPRODPATH}/..
    git clone -b master https://github.com/cms-sw/genproductions.git genproductions 
    cd ${GENPRODPATH}
    # Checkout the repo so that it corresponds to some fixed point in development
    git checkout 91b1bfb5f363d9d07c66d21a20dae19a0bb272d2
    # Copy relevant code  
    for FILE in addons helpers scanfiles gridpack_generation.sh diagram_generation.sh clean_diagrams.sh submit_madpack_ttbareft.sh configure_gridpack.py transfer_gridpacks.py transfer_diagrams.py submit_cmsconnect_gridpack_generation.sh ; do 
	cp -r ${EFTMCPATH}/${FILE} ${GENPRODPATH}/bin/MadGraph5_aMCatNLO/.
    done
    cd ${GENPRODPATH}/.
    echo "You are done with setting up genproduction for EFT gridpack generation!"
fi                           

