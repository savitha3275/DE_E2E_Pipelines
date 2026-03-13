
# Kafka pipeline 

Requirements:
Kafka cluster running (Docker Compose)
Python 3.9+
Packages: confluent-kafka, pandas, streamlit

**error : no module found**

**3.12 required , found 3.9**

"it works on my machine"

Docker solves this problem --?> by packaging the core dependancies, and os into a single PORTABLE UNIT --> DOCKER

### Key concepts

Image : read-only template which describes what should be inside a container. 

Container : Running instance of an image. If image is the recipe ---> container is the cooked dish . 

Dockerfile :  text file with instructions to build an image.  Recipe card, that docker follows step by step 

Docker Hub : github for docker images . 

dbt project repo -> Image -- the code

Container --> running dbt jobs

dockerfile --> setup instructions 


### Dockers and virtual machines

Boot time : minutes (VM) : container (seconds)

Size : GB - VM .... MB to GB -- container..

OS : guest OS ver VM  -- Container : Shares host OS kernel

use case : VM : Running windows on mac ..... Container : Microservices and data pipelines

if docker file is in some other folder : docker build -t my-first-image ./some-other-folder'

#!/bin/bash : shebang 

cat > greet.sh << 'EOF'

#!/bin/bash

echo "=================="

echo 



EOF


Fluxcart is e-commerce platform.

every time a user browses a product, places an order or makes a paymnet --> is a event

**kafka === post office**

**topics --> mailboxes**

**producers --> people who drop letters(events) into the mailboxes**

**consumers --> people who pick those letters(events) form the mailboxes**

**brokers --> post office building (servers) that store the mailboxes**

Docker and kafka cluster

we need 3 brokers, we create it on docker-compose.yml. which means 3 nodes(broker) in one machine(cluster)

docker-compose.yml --> cluster config

config.py --> shared foundation 

models.py --> event design before producer 

setup_topics.py --> topics must exist before producer can write

producer.py --> write events before teaching how to read them

base_consumer.py --> loop everything

analytics.py 

fraud.py 

inventory.py

run_pipeline.py. --> run everything together

run dashboard.py --> to view the pipeline data in dashboard

run export.py --> to export the data to csv files for bi integration

run  streamlit run streamlit_pipeline.py --> to stream events to streamlit dashboard

steps to run :
docker compose up -d

then run,
docker ps --> check if all the brokers are up and running

then run, 
docker exec fluxcart-broker-1 kafka-topics --bootstrap-server localhost:9092 --list(shows nothing in o/p which means no topics)

then run,
python setup_topics.py (in terminal 1)

then run,
python run_pipeline.py (in terminal 2)

then run,
python export.py (in terminal 3. go inside bi_integration folder and run this)

then finally, run
streamlit run streamlit_pipeline.py







