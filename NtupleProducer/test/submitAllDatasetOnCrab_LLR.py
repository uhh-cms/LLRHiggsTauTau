import os
import sys
import re

# NB: you need to source crab environment before lauching this script:
# source /cvmfs/cms.cern.ch/crab3/crab.sh

###################################################################
#### Parameters to be changed for each production

YEAR = 2018
assert YEAR in (2016, 2017, 2018)

PERIOD = "" # 'postVFP' # can be left empty if running on 2017 and 2018
assert PERIOD in ("", "postVFP")

PREFIX = "MC"
assert PREFIX in ("Sig", "MC", "Data")
TAG = "TestWeightSchemes"

period16 = "APV" if (PERIOD=="" and YEAR==2016) else ""
datasetsFile = "datasets_UL" + str(YEAR)[-2:] + period16 + ".txt"
nolocFile = "datasets_UL" + str(YEAR)[-2:] + ".noloc.txt"
tag = PREFIX + "_UL" + str(YEAR)[-2:] + period16 + "_" + TAG

if YEAR == 2016:
    lumiMaskFileName = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt'

elif YEAR == 2017:
    lumiMaskFileName = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt'

elif YEAR == 2018:
    lumiMaskFileName = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt'
    
# /!\ Be sure that the IsMC flag in analyzer_LLR.py matches this one!
isMC = True if PREFIX in ("Sig", "MC") else False 

PROCESS = [
    "BACKGROUNDS_TT_" + str(YEAR) + period16,
    "BACKGROUNDS_WJETS_" + str(YEAR) + period16,
    "BACKGROUNDS_DY_NLO_" + str(YEAR) + period16,
    #"BACKGROUNDS_DY_NLO_PTSLICED_" + str(YEAR) + period16,
    "BACKGROUNDS_DY_" + str(YEAR) + period16,
    "BACKGROUNDS_VV_" + str(YEAR) + period16,
    "BACKGROUNDS_VVV_" + str(YEAR) + period16,
    "BACKGROUNDS_ST_" + str(YEAR) + period16,
    "BACKGROUNDS_EWK_" + str(YEAR) + period16,
    "BACKGROUNDS_H_" + str(YEAR) + period16,
    "BACKGROUNDS_TTX_" + str(YEAR) + period16,
    "BACKGROUNDS_TTVH_" + str(YEAR) + period16,
    #"BACKGROUNDS_DY_QQ_HTSLICED_" + str(YEAR) + period16
    "BACKGROUNDS_HH_" + str(YEAR) + period16
    # "BACKGROUNDS_DY_LM_" + str(YEAR) + period16

    # "SIGNALS_GF_SPIN0_" + str(YEAR) + period16,
    # "SIGNALS_GF_SPIN2_" + str(YEAR) + period16,
    # "SIGNALS_HY_" + str(YEAR) + period16,
]

if not isMC:
    PROCESS = [
        "DATA_TAU_" + str(YEAR) + period16,
        "DATA_ELE_" + str(YEAR) + period16,
        "DATA_MU_" + str(YEAR) + period16,
        "DATA_MET_" + str(YEAR) + period16,
        "DATA_DOUBLEMU_" + str(YEAR) + period16
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
        line = line.strip() # remove newline at the end and leading/trailing whitespaces
        
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
                assert len(words)==2
                dtsetToLaunch.append(words)

# CREATE CRAB JOBS
os.system ("voms-proxy-init -voms cms")

for name in PROCESS: crabJobsFolder + "_" + name
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
for dtset in dtsetToLaunch:

    ignoreLoc = False
#    with open(nolocFile) as fnoLoc:
#        if dtset in fnoLoc.read():
#            ignoreLoc = True
#            print("ignoring locality for dset: {}".format(dtset))

    dtsetName = dtset[0]
    uncertScheme = dtset[1]
    if '/MINIAODSIM' in dtsetName:
        dtsetName = dtsetName.replace('/MINIAODSIM', "")
    elif '/MINIAOD' in dtsetName:
        dtsetName = dtsetName.replace('/MINIAOD', "")
    dtsetName = dtsetName.replace('/', "__")
    dtsetName = dtsetName.strip("__") # remove leading and trailing double __
    shortName = dtsetName.split('__')[0]
    
    if (len(shortName) > 95): # requestName not exceed 100 Characters!
        toRemove = len (shortName) - 95
        shortName = shortName[toRemove:]

    static_defs_file = "crab3_template_LLR.py"
    mes = '\n'.join(('# Template generated on-the-fly by submitAllDatasetOnCrab_LLR.py and used for automatic script submission of multiple datasets',
                     '',
                     'from WMCore.Configuration import Configuration',
                     'config = Configuration()',
                     '',
                     'config.section_("General")',
                     'config.General.requestName = "DefaultReqName"',
                     'config.General.workArea = "DefaultCrab3Area"',
                     '',
                     'config.section_("JobType")',
                     'config.JobType.pluginName = "Analysis"',
                     'config.JobType.psetName = "analyzer_LLR.py" # to produce LLR ntuples or EnrichedMiniAOD according to the RunNtuplizer bool',
                     'config.JobType.pyCfgParams=["uncertaintyScheme", "{}"]'.format(uncertScheme),
                     '',
                     'config.JobType.sendExternalFolder = True # Needed until the PR including the Spring16 ele MVA ID is integrated in CMSSW/cms-data.',
                     'config.JobType.inputFiles=["JECUncertaintySources"] # FRA: adding to the sandobx the directory with JEC files (https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#How_are_the_inputFiles_handled_i)',
                     '',
                     'config.section_("Data")',
                     'config.Data.inputDataset = "/my/precious/dataset"',
                     'config.Data.inputDBS = "global"',
                     'config.Data.splitting = "Automatic"',
                     '#config.Data.unitsPerJob = 28000 #number of events per jobs # 18K FOR SINGLE ELE, 10k for others, 28K for muons',
                     'config.Data.totalUnits = -1 #number of event',
                     'config.Data.publication = True',
                     'config.Data.outputDatasetTag = "DefaultPublishName"',
                     '',
                     '#private 2K cores',
                     '#config.section_("Debug")',
                     '',
                     'config.section_("Site")',
                     'config.Site.storageSite = "T2_FR_GRIF_LLR" # PARIGI',
                     '#config.Site.storageSite = "T3_IT_MIB" # MILANO',
                     ))

    with open(static_defs_file, 'w') as sdf:
        sdf.write(mes)
        
    command = ' '.join(("crab submit -c {}".format(static_defs_file),
                        " General.requestName=%s" % (shortName + "_" + str(counter)),
                        " General.workArea=%s" % crabJobsFolder,
                        " Data.inputDataset=%s" % dtset[0],
                        " Data.outLFNDirBase=/store/user/bfontana/HHNtuples_res/UL" + str(YEAR) + period16 + "/%s/%s" % (tag, str(counter)+"_"+dtsetName),
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
