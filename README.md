# Multicultural City

## COMP90024 Cluster and Cloud Computing - Assignment 1 Semester 1 2022

This application identify number of different languages used in Tweets and their location in the location grids.
It has parallel implementation so that it can run on multiple processors.

The application takes the following input files:
* A Twitter JSON file that contains information about Tweets to be allocated into a location grid. See example file `data/localTwitter.json`.
* A JSON file contains a list of rectangular location grids represented in Polygon coordinates. See example file `data/sydGrid-2.json`.
* You can optionally provide a JSON file to map a Twitter language code to a language name. See example file `data/language.json`.

For more information, run:

```
python src/app.py -h
```

### Installation

It requires Python 3.7 or higher.
```
pip install -r requirements.txt
```

### Run on Spartan:
Slurm scripts to run on Spartan has been provided for different configurations:

1 node, 1 core:
```
sbatch run1c.slurm
```

1 node, 8 cores:
```
sbatch run8c1n.slurm
```

2 nodes, 8 cores:
```
sbatch run8c2n.slurm
```

### Local environment:

A small example of expected Twitter and grid JSON files are provided in the `/data` folder.

To run on your local environment:

```bash
mpirun -n 5 python src/app.py -f data/localTwitter.json --grid data/sydGrid-2.json
```
