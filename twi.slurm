'''
#!/bin/bash

#SBATCH --job-name=twi
#SBATCH --output=twi.txt
#SBATCH --error=twi.err
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mail-user=fzzhou1@student.unimelb.edu.au
#SBATCH --mail-type=ALL

module load python/3.8.6
model load mpi4py/3.0.3

'''


#!/bin/bash
#SBATCH --job-name=my_job_name
#SBATCH --output=output.log
#SBATCH --error=errors.log

# First configuration: 1 node, 1 core
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

echo "Running job with 1 node, 1 core"
mpirun -np 1 python trial.py

# Second configuration: 1 node, 8 cores
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8

echo "Running job with 1 node, 8 cores"
mpirun -np 8 python trial.py

# Third configuration: 2 nodes, 4 cores per node
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

echo "Running job with 2 nodes, 4 cores per node"
mpirun -np 8 -hostfile $SLURM_NODEFILE python trial.py

