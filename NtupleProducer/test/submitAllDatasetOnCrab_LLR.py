import os
import sys
import re

# NB: you need to source crab environment before lauching this script:
# source /cvmfs/cms.cern.ch/crab3/crab.sh

###################################################################
#### Parameters to be changed for each production

YEAR = "16"
assert YEAR in ("16", "17", "18")

PERIOD = '' # 'APV' # can be left empty if running on 2017 and 2018
assert PERIOD in ("", "APV")

PREFIX = "MC"
assert PREFIX in ("Sig", "MC", "Data")
TAG = "April2024"

datasetsFile = "datasets_UL" + YEAR + PERIOD + ".txt"
nolocFile = "datasets_UL" + YEAR + ".noloc.txt"
tag = PREFIX + "_UL" + YEAR + PERIOD + "_" + TAG

if YEAR == "16":
    lumiMaskFileName = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt'

elif YEAR == "17":
    lumiMaskFileName = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt'

elif YEAR == "18":
    lumiMaskFileName = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt'
    
# /!\ Be sure that the IsMC flag in analyzer_LLR.py matches this one!
isMC = True if PREFIX in ("Sig", "MC") else False 

PROCESS = [
    "BACKGROUNDS_TT_20" + YEAR + PERIOD,
    "BACKGROUNDS_WJETS_20" + YEAR + PERIOD,
    "BACKGROUNDS_DY_NLO_20" + YEAR + PERIOD,
    #"BACKGROUNDS_DY_NLO_PTSLICED_20" + YEAR + PERIOD,
    "BACKGROUNDS_DY_20" + YEAR + PERIOD,
    "BACKGROUNDS_VV_20" + YEAR + PERIOD,
    "BACKGROUNDS_VVV_20" + YEAR + PERIOD,
    "BACKGROUNDS_ST_20" + YEAR + PERIOD,
    "BACKGROUNDS_EWK_20" + YEAR + PERIOD,
    "BACKGROUNDS_H_20" + YEAR + PERIOD,
    "BACKGROUNDS_TTX_20" + YEAR + PERIOD,
    "BACKGROUNDS_TTVH_20" + YEAR + PERIOD,
    #"BACKGROUNDS_DY_QQ_HTSLICED_20" + YEAR + PERIOD
    "BACKGROUNDS_HH_20" + YEAR + PERIOD
    # "BACKGROUNDS_DY_LM_20" + YEAR + PERIOD

    # "SIGNALS_GF_SPIN0_20" + YEAR + PERIOD,
    # "SIGNALS_GF_SPIN2_20" + YEAR + PERIOD,
    # "SIGNALS_HY_20" + YEAR + PERIOD,
]

if not isMC:
    PROCESS = [
        "DATA_TAU_20" + YEAR + PERIOD,
        "DATA_ELE_20" + YEAR + PERIOD,
        "DATA_MU_20" + YEAR + PERIOD,
        "DATA_MET_20" + YEAR + PERIOD,
        "DATA_DOUBLEMU_20" + YEAR + PERIOD
    ]

FastJobs = False # controls number of jobs - true if skipping SVfit, false if computing it (jobs will be smaller)
VeryLong = False # controls time for each job - set to true if jobs contain many real lepton pairs --> request for more grid time
EnrichedToNtuples = False # use only False! Do not create ntuples on CRAB because it is very slow, use tier3
PublishDataset = False # publish dataset; set to false if producing ntuples

###################################################################
#### Automated script starting

# dataset block definition
sectionBeginEnd = "==="

if EnrichedToNtuples: PublishDataset = False


# check if file with dataset exist
if not os.path.isfile(datasetsFile):
    print "File %s not found!!!" % datasetsFile
    sys.exit()

#check if directory exists
crabJobsFolder = "crab3_" + tag
# if os.path.isdir(crabJobsFolder):
#     print "Folder %s already exists, please change tag name or delete it" % crabJobsFolder
#     sys.exit()

# grep all datasets names, skip lines with # as a comment
# block between === * === are "sections" to be processed

currSection = ""
dtsetToLaunch = []

print " =========  Starting submission on CRAB ========"
print " Parameters: "
print " PROCESS: "
for pr in PROCESS: print "   * " , pr
print " tag: " , tag
print " Fast jobs?: " , FastJobs
print " Publish?: "   , PublishDataset

# READ INPUT FILE
with open(datasetsFile) as fIn:

    for line in fIn:
        line = line.strip() # remove newline at the end and leding/trailing whitespaces
        
        if not line: #skip empty lines
            continue

        if "#" in line: #commented line
            continue

        words = line.split()

        if len(words) >= 3:
            if words[0] == sectionBeginEnd and words[2] == sectionBeginEnd: 
                currSection = words[1]
        else:
            if currSection in PROCESS:
                dtsetToLaunch.append(line)

# CREATE CRAB JOBS
os.system ("voms-proxy-init -voms cms")

for name in PROCESS: crabJobsFolder + "_" + name
print crabJobsFolder
os.system ("mkdir %s" % crabJobsFolder)

counter = 1 # appended to the request name to avoid overlaps between datasets with same name e.g. /DoubleEG/Run2015B-17Jul2015-v1/MINIAOD vs /DoubleEG/Run2015B-PromptReco-v1/MINIAOD

outlog_name = os.path.join(crabJobsFolder, "submissionLog.txt")
with open(outlog_name, "w") as outlog:
    outlog.write(" =========  Starting submission on CRAB ========\n")
    outlog.write(" Parameters: \n")
    outlog.write(" PROCESS: \n")
    for pr in PROCESS:
        outlog.write("   * %s\n" % pr)
    outlog.write(" tag: %s\n" % tag)
    outlog.write(" Fast jobs?: %s\n" % str(FastJobs))
    outlog.write(" Publish?: %s\n"   % str(PublishDataset))
    outlog.write(" ===============================================\n\n\n")

site_white_list = [
    "T1_DE_KIT", "T2_DE_DESY",
    "T2_CH_CERN",
    "T1_US_FNAL", "T2_US_Caltech", "T2_US_Vanderbilt", "T2_US_Wisconsin",
]

from collections import OrderedDict
crab_whitelistarg = OrderedDict()
crab_whitelistarg["Site.whitelist"] = site_white_list
print dtsetToLaunch
for dtset in dtsetToLaunch:

    ignoreLoc = False
#    with open(nolocFile) as fnoLoc:
#        if dtset in fnoLoc.read():
#            ignoreLoc = True
#            print("ignoring locality for dset: {}".format(dtset))

    dtsetNames = dtset
    if '/MINIAODSIM' in dtset:
        dtsetNames = dtset.replace('/MINIAODSIM', "")
    elif '/MINIAOD' in dtset:
        dtsetNames = dtset.replace('/MINIAOD', "")
    dtsetNames = dtsetNames.replace('/', "__")
    dtsetNames = dtsetNames.strip("__") # remove leading and trailing double __ 
    shortName = dtset.split('/')[1]

    if (len(shortName) > 95): # requestName not exceed 100 Characters!
        toRemove = len (shortName) - 95
        shortName = shortName[toRemove:]

    command = ' '.join(("crab submit -c crab3_template_LLR.py",
                        " General.requestName=%s" % (shortName + "_" + str(counter)),
                        " General.workArea=%s" % crabJobsFolder,
                        " Data.inputDataset=%s" % dtset,
                        " Data.outLFNDirBase=/store/user/bfontana/HHNtuples_res/UL" + YEAR + "/%s/%s" % (tag, str(counter)+"_"+dtsetNames),
                        " Data.outputDatasetTag=%s" % (shortName + "_" + tag + "_" + str(counter)),
                        " Data.splitting='Automatic'",
                        " Data.splitting='FileBased'",
                        " Data.unitsPerJob=2",
                        " Data.totalUnits=-1"))
    if ignoreLoc:
        command += " Data.ignoreLocality=True"
        #command += " Site.whitelist={}".format(site_white_list)
        fmt_list = lambda a: "[{}]".format(",".join(map("'{}'".format, a)))
        cms_run_arg = lambda k, a: "{}=\"{}\"".format(k, fmt_list(a) if isinstance(a, list) else a)
        command += " " + " ".join(cms_run_arg(*tpl) for tpl in crab_whitelistarg.items())

    if EnrichedToNtuples:
        command += " Data.inputDBS=phys03" # if I published the dataset need to switch from global (default)
        command += " JobType.psetName=ntuplizer.py" # run a different python config for enriched

    if not PublishDataset:
        command += " Data.publication=False" # cannot publish flat root ntuples

    if FastJobs:
        command += " Data.unitsPerJob=100000" # circa 50 ev / secondo --> circa 1/2 h ; else leave default of 4000 jobs
    if VeryLong:
        command += " JobType.maxJobRuntimeMin=2500" # 32 hours, default is 22 hours -- can do up to 2800 hrs
    if not isMC:
        command += " Data.lumiMask=%s" % lumiMaskFileName
        
    print command,  "\n"
    os.system(command)

    with open(outlog_name, "w") as outlog:
        outlog.write(command + "\n\n")

    counter = counter + 1
