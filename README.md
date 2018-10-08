# hyphe2text
Python scripts to extract text from Hyphe's MongoDB

## Install

Dependencies: PyMongo
```
pip install pymongo
```

## Run the script

### 1. A port must be open to the Hyphe's mongodb.

The usual port is 27017 and you can check that it is open by looking at ```http://localhost:27017```.

*In the case of a Docker install* you probably have to edit the configuration to open the port. Open the file ```docker-compose.yml``` and in the part related to MongoDB, add the following lines:
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

### 2. 
