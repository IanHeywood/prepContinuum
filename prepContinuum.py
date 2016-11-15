# -----------------------------------------------------------
# Initial processing of ASKAP data ahead of continuum selfcal / imaging.
# 
# Usage: python prepContinuum.py CALSB SCISB NBEAMS NCHANNELS
#
# This is a self-contained script._
#
# Feed it the SB numbers of the calibrator and science targets, and the number of beams.
# Source the submit_*.sh script and some time later it will have produced per-beam 
# Measurement Sets that are bandpass calibrated, flagged, frequency averaged to 0.5 MHz
# channels and have a WEIGHT_SPECTRUM column.
#
# Source the cleanup_*.sh script afterwards and it should remove all the intermediate
# scripts and products.
#
# This will only run on Galaxy.
# 
# ian.heywood@csiro.au
# last mod 15.11.16
# -----------------------------------------------------------


# -----------------------------------------------------------
#
# Import functions, check inputs, find Measurement Sets...
#
# -----------------------------------------------------------

import sys
import os
import numpy
import glob

calsb = sys.argv[1]
scisb = sys.argv[2]
nbeams = int(sys.argv[3])
nchans = int(sys.argv[4])

sbpath = '/scratch2/askap/askapops/askap-scheduling-blocks/'
slurmpath = 'slurmfiles/'

print ''
print 'Calibration SB:     ',calsb
print 'Science target SB:  ',scisb
print 'Number of beams:    ',nbeams
print 'Number of channels: ',nchans

if sbpath[-1] != '/':
	sbpath += '/'

runfile = 'submit_cal'+calsb+'_sci'+scisb+'.sh'
cleanup = 'cleanup_cal'+calsb+'_sci'+scisb+'.sh'

calms = glob.glob(sbpath+calsb+'/20*.ms')
scims = glob.glob(sbpath+scisb+'/20*.ms')

if len(calms) != 1 or len(scims) != 1:
	print ''
	print 'Error finding unique calibration or target Measurement Sets'
	print ''
	sys.exit(-1)
else:
	calms = calms[0]
	scims = scims[0]

print ''
print 'Found cal MS:      ',calms
print 'Found target MS:   ',scims
print ''

if not os.path.isdir(slurmpath):
	print 'Creating folder:   ',slurmpath
	os.system('mkdir '+slurmpath)
else:
	print 'Script folder:     ',slurmpath
print ''


# -----------------------------------------------------------
#
# Define functions to write scripts
#
# -----------------------------------------------------------


def write_mssplit_config(configfile,beam,inms,opms,cal):
	f = open(configfile,'w')
	f.writelines(['vis = '+inms+'\n',
		'outputvis = '+opms+'\n',
		'stman.bucketsize = 1048576\n',
		'stman.tilenchan = 54\n'])
	if cal:
		f.writelines(['scans = ['+str(beam)+']\n'])
	#          'fieldnames = [B1934-638_beam'+str(beam)+']'        
	f.writelines(['beams = ['+str(beam)+']\n',
		'channel = 1-'+str(nchans)+'\n',
		'width = 1\n'])
	f.close()
	return configfile


def write_mssplit_slurm(slurmfile,config):
	f = open(slurmfile,'w')
	f.writelines(['#!/bin/bash -l\n',
		'#SBATCH --partition=workq\n',
		'#SBATCH --clusters=galaxy\n',
		'#SBATCH --time=02:00:00\n',
		'#SBATCH --ntasks=1\n',
		'#SBATCH --ntasks-per-node=1\n',
		'#SBATCH --job-name=mssplit\n',
		'#SBATCH --export=NONE\n',
		#        '#SBATCH --account=ja3'
		'\n',
		'module load askapsoft\n',
		'\n',
		'aprun -n 1 -N 1 mssplit -c '+config+'\n'])
	f.close() 
	return slurmfile


def write_CASA_slurm(slurmfile,casascript,jobname,timeout,msname):
	f = open(slurmfile,'w')
	f.writelines(['#!/bin/bash -l\n',
		'#SBATCH --partition=workq\n',
		'#SBATCH --clusters=galaxy\n',
		'#SBATCH --time='+timeout+'\n',
		'#SBATCH --ntasks=1\n',
		'#SBATCH --ntasks-per-node=1\n',
		'#SBATCH --job-name='+jobname+'\n',
		'#SBATCH --export=NONE\n',
		'#SBATCH --mem=16G\n',
		#'#SBATCH --account=ja3\n',
		'\n',
		'export OMP_NUM_THREADS=1\n',
		'module unload casa\n',
		'module load casa/4.5.0-el5\n',
		'export SCRIPTS=$PWD\n'
		#'msnum=$SLURM_ARRAY_TASK_ID\n'
		#'ms=${!msnum}\n'
		'mkdir out_'+msname+'\n'
		'cd out_'+msname+'\n'
		'\n',
		'aprun -b casa -n 1 -N 1 -ss casapy --nogui --nologger --log2term ',
		' -c $SCRIPTS/'+casascript+' > mycasalog_X${SLURM_JOB_ID}.log'])
	f.close()
	return slurmfile


def write_CASA_process_cal(casascript,msname):
	# flag, setjy, bandpass
	f = open(casascript,'w')
	f.writelines(['import os\n',
		'myms = "../'+msname+'"\n',
		'soloms = myms.split("/")[-1]\n',
		'beam = soloms.split("_")[3].replace("beam","")\n',
		'caltab = "cal_"+soloms+".B"\n',
		'os.system("ln -s "+myms+" .")\n',
		'flagdata(vis=soloms,mode="manual",autocorr=True)\n',
		'flagdata(vis=soloms,mode="rflag",datacolumn="DATA")\n',
		'flagdata(vis=soloms,mode="tfcrop",datacolumn="DATA")\n',
		'setjy(vis=soloms,field=beam,usescratch=True,standard="Perley-Butler 2010")\n',
		'bandpass(vis=myms,field=beam,caltable=caltab,solnorm=False)\n',
		'os.system("mv "+caltab+" ../")\n'])
	f.close()
	return casascript


def write_CASA_process_sci(casascript,msname):
	# applycal, mstransform
	f = open(casascript,'w')
	f.writelines(['import os\n',
		'import glob\n',
		'import numpy\n',
		'myms = "../'+msname+'"\n',
		'soloms = myms.split("/")[-1]\n',
		'beam = soloms.split("_")[3]\n',
		'caltab = glob.glob("../cal_*"+beam+"*.B")[0]\n',
		'os.system("ln -s "+myms+" .")\n',
		'flagdata(vis=soloms,mode="manual",autocorr=True)\n',
		'flagdata(vis=soloms,mode="rflag",datacolumn="DATA")\n',
		'flagdata(vis=soloms,mode="tfcrop",datacolumn="DATA")\n',
		'tb.open(soloms+"/FIELD")\n',
		'flds = tb.getcol("NAME")\n',
		'tb.done()\n',
		'applycal(vis=soloms,gaintable=caltab,interp=["nearest"])\n',
		'for i in range(0,len(flds)):\n',
		'    opms = soloms.replace("_sci","_"+flds[i]+"_wtspec")\n',
		'    mstransform(vis=soloms,outputvis=opms,field=str(i),datacolumn="corrected",chanaverage=True,chanbin=27,usewtspectrum=True,realmodelcol=True)\n'])
	#	'    os.system("mv "+opms+" ../")\n'])
	f.close()
	return casascript


# -----------------------------------------------------------
#
# Setup lists of product Measurement Sets
#
# -----------------------------------------------------------


cal_ms_list = []
sci_ms_list = []

for beam in range(0,nbeams):
	cal_ms_list.append('SB'+calsb+'_'+calms.split('/')[-1].replace('.ms','_beam'+str(beam)+'_cal.ms'))
	sci_ms_list.append('SB'+scisb+'_'+scims.split('/')[-1].replace('.ms','_beam'+str(beam)+'_sci.ms'))


# -----------------------------------------------------------
#
# Write the job submission script
#
# -----------------------------------------------------------


f = open(runfile,'w')

for beam in range(0,nbeams):

	# -----------------------------------------------------------
	#
	# JOB IDs and per-beam MSs
	#
	cal_split_id = 'CAL_SPLIT_BEAM'+str(beam)+'_ID'
	sci_split_id = 'SCI_SPLIT_BEAM'+str(beam)+'_ID'
	cal_casa1_id = 'CAL_CASA1_BEAM'+str(beam)+'_ID'
	sci_casa1_id = 'SCI_CASA1_BEAM'+str(beam)+'_ID'
	sci_casa2_id = 'SCI_CASA2_BEAM'+str(beam)+'_ID'

	opms_cal = cal_ms_list[beam]
	opms_sci = sci_ms_list[beam]
	
	#
	# -----------------------------------------------------------
	#
	# (1) SPLIT CALIBRATOR BEAM
	#
	# GENERATE SCRIPTS
	#
	slurm = slurmpath+'split_cal_beam'+str(beam)+'.slurm'
	config = slurmpath+'config_split_cal_beam'+str(beam)+'.in'
	write_mssplit_config(config,beam,calms,opms_cal,True)
	write_mssplit_slurm(slurm,config)
	#
	# DEPENDENCIES: None
	#
	f.write(cal_split_id+"=`sbatch "+slurm+" | awk '{print $4}'`\n") 
	#
	# -----------------------------------------------------------
	#
	# (2) SPLIT TARGET BEAM
	#
	# GENERATE SCRIPTS
	#
	slurm = slurmpath+'split_sci_beam'+str(beam)+'.slurm'
	config = slurmpath+'config_split_sci_beam'+str(beam)+'.in'
	write_mssplit_config(config,beam,scims,opms_sci,False)
	write_mssplit_slurm(slurm,config)
	#
	# DEPENDENCIES: None
	#
	f.write(sci_split_id+"=`sbatch "+slurm+" | awk '{print $4}'`\n") 
	#
	# -----------------------------------------------------------
	#
	# (3) RUN CAL CASA SCRIPT
	#
	# GENERATE SCRIPTS
	#
	cal_casa1 = slurmpath+'casa_cal_1_beam'+str(beam)+'.py'
	cal_slurm1 = slurmpath+'cal_1_beam'+str(beam)+'.slurm'
	write_CASA_process_cal(cal_casa1,opms_cal)
	write_CASA_slurm(cal_slurm1,cal_casa1,'proc_cal_'+str(calsb),'04:00:00',opms_cal)
	#
	# DEPENDENCIES: cal_split_id
	#
	f.write(cal_casa1_id+"=`sbatch -d afterok:${"+cal_split_id+"} "+cal_slurm1+" "+opms_cal+" | awk '{print $4}'`\n")
	#
	# -----------------------------------------------------------
	#
	# (4) RUN SCI CASA SCRIPT
	#
	# GENERATE SCRIPTS
	#
	sci_casa2 = slurmpath+'casa_sci_2_beam'+str(beam)+'.py'
	sci_slurm2 = slurmpath+'sci_2_beam'+str(beam)+'.slurm'
	write_CASA_process_sci(sci_casa2,opms_sci)
	write_CASA_slurm(sci_slurm2,sci_casa2,'cal_sci_'+str(scisb),'06:00:00',opms_sci)
	#
	# DEPENDENCIES: sci_casa1_id:cal_casa1_id
	#
	f.write(sci_casa2_id+"=`sbatch -d afterok:${"+sci_split_id+"}:${"+cal_casa1_id+"} "+sci_slurm2+" "+opms_sci+" | awk '{print $4}'`\n")

f.close()


# -----------------------------------------------------------
#
# Write the cleanup script
#
# -----------------------------------------------------------


f = open(cleanup,'w')
for item in cal_ms_list:
	f.write('rm -rf '+item+'\n')
for item in sci_ms_list:
	f.write('rm -rf '+item+'\n')
f.write('rm -rf *.flagversions\n')
f.write('rm -rf cal_*.B\n')
f.write('rm -rf '+slurmpath+'\n')
f.write('rm -rf out_*.ms\n')
f.write('rm -rf slurm*.out\n')
f.close()


# -----------------------------------------------------------
#
# End
#
# -----------------------------------------------------------


print 'Submission script: ',runfile
print 'Cleanup script:    ',cleanup
print ''
