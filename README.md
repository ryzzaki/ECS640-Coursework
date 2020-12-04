## Replication Steps

```bash
# ssh into the cluster
ssh <username>@<cluster>.eecs.qmul.ac.uk

# load python
module load python/3.7.7

# create the env
python -m venv env

# activate the env
source env/bin/activate

# install the dependencies
pip install -r requirements.txt

# run in the hadoop cluster
python ./scripts/<script>.py -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/<file_name> > ./output/<output>.txt

# deactivate the env
deactivate
```

## Replication Steps using Pipenv

```bash
# install the dependencies
pipenv install -r requirements.txt

# run the *.py files using pipenv
pipenv python ./scripts/<script>.py
```
