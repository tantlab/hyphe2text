# Hyphe2Text
Python scripts to extract text from Hyphe's MongoDB

This script has two modes:
* Index the text as a bunch of files
* Index it in an ElasticSearch

You can do each or both at the same time.

## You need a Hyphe instance

This script relies on an existing Hyphe server running. See [Hyphe's repository](https://github.com/medialab/hyphe).

The script will retrieve data from that instance and store it on your hard disk drive.

## Install

Create a virtual environment (here named "h2t") for [Goose](https://github.com/grangier/python-goose) (the text extraction library) and Hyphe2Text. 

```
mkvirtualenv --no-site-packages h2t
```

Clone the Goose repo, browse to it and install it.

```
git clone https://github.com/grangier/python-goose.git
cd python-goose
pip install -r requirements.txt
python setup.py install
cd ..
```

Then git clone this repository (hyphe2text) and install the requirements in the virtual environment:

```
git clone https://github.com/tantlab/hyphe2text.git
cd hyphe2text
pip install -r requirements.txt
```

## Run the script

### 1. Check the port to Hyphe's MongoDB

A port to Hyphe's MongoDB has to be open. The usual port is 27017 and you can check it by looking at ```http://localhost:27017```.

**In the case of a Docker install** you probably have to edit the configuration to open the port. Open the file ```docker-compose.yml``` and in the part related to MongoDB, add the following lines:
```yml
    ports:
      - "27017:27017"
```
This part of the file will probably look like that:
```yml
(...)
  mongo:
    restart: ${RESTART_POLICY}
    image: mongo:3.0
    volumes:
      - ${DATA_PATH}mongo-data:/data/db
    ports:
      - "27017:27017"
```

Don't forget to restart the Docker:
```
$  docker-compose restart
```

### 2. Edit the config file to match your situation
_TO DO: write the config info. Also, create the config file. Upcoming!_

### 3. Run Hyphe and open the corpus

Hyphe's MongoDB can be mined without the corpus running, however only Hyphe's [Traph](https://github.com/medialab/hyphe-traph) knows to which webentity each page belongs. The script queries Hyphe's corpus via the API, hence it must be switched on.

### 4. Run the script
```
python hyphe2text.py
```
It takes some time; be patient!