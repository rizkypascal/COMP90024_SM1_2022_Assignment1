# COMP90024_SM1_2022_Assignment1

## Run on Spartan:
Slurm scripts to run on Spartan has been provided:

1 node, 1 core:
```
sbatch run1c.slrum
```

1 node, 8 cores:
```
sbatch run8c1n.slrum
```

2 nodes, 8 cores:
```
sbatch run8c2n.slrum
```

## Local environment:

A small example of expected Twitter and grid JSON files are provided in the `/data` folder.

To install on your local environment:
```
pip install -r requirements.txt
```

To run on your local environment:

```bash
mpirun -n 5 python src/app.py -f data/localTwitter.json --grid data/sydGrid-2.json
```