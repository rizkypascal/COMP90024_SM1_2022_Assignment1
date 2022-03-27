#!/bin/bash

dir=/home/kesumadewij/assignment1

#SBATCH --nodes=2
#SBATCH --ntasks=8
module load foss/2019b
module load python/3.7.4
module load mpi4py/3.0.2-timed-pingpong

srun -n 8 python3 src/app.py -f $dir/bigTwitter.json --grid $dir/sydGrid.json

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s