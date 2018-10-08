# hyphe2text
Python scripts to extract text from Hyphe's MongoDB

## dependencies

### HYPHE

This script relies on an existing Hyphe server running.
see https://github.com/medialab/Hypertext-Corpus-Initiative

### Python requirements

pymongo

## Install

git clone this repository

Then just execute:

```
$ pip install -r requirements.txt
```

## Run the script

### 1. Check the port to Hyphe's MongoDB

A port to Hyphe's MongoDB has to be open. The usual port is 27017 and you can check it by looking at ```http://localhost:27017```.

_In the case of a Docker install_ you probably have to edit the configuration to open the port. Open the file ```docker-compose.yml``` and in the part related to MongoDB, add the following lines:
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
_TODO_

### 3. Run Hyphe and open the corpus

The MongoDB can be mined without the corpus running, however only Hyphe's core knows whose webentity is each page. The script queries Hyphe's corpus via the API, hence it must be switched on.

### 4. Run the script
_TODO_