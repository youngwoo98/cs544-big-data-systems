# These steps below need to be manually done after cloning (copied from p5 description)

## Cluster Setup

### Virtual Machine

~4 GB is barely enough for P5. Before you start, take a moment to enable a 1
GB swap file to supplement.  A swap file is on storage, but acts
as extra memory. This has performance implications as storage is
much slower than RAM (as what we have studied in class).  


```
# https://www.digitalocean.com/community/tutorials/how-to-add-swap-space-on-ubuntu-22-04
sudo fallocate -l 1G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
# htop should show 1 GB of swap beneath memory
```

### Containers

For this project, you'll deploy a small cluster of containers:

* `p5-nb` (1 Jupyter container)
* `p5-nn` (1 NameNode container)
* `p5-dn` (1 DataNode container)
* `p5-boss` (1 Spark boss container)
* `p5-worker` (2 Spark worker containers)

You should be able to build all these images like this:

```
docker build . -f p5-base.Dockerfile -t p5-base
docker build . -f notebook.Dockerfile -t p5-nb
docker build . -f namenode.Dockerfile -t p5-nn
docker build . -f datanode.Dockerfile -t p5-dn
docker build . -f boss.Dockerfile -t p5-boss
docker build . -f worker.Dockerfile -t p5-worker
```

We provide most of the Dockerfiles mentioned above, but you'll need to write 
`boss.Dockerfile` and `worker.Dockerfile` yourself. These Dockerfiles will invoke
the Spark boss and workers and will use `p5-base` as their base Docker image.

To start the Spark boss and workers, you will need to run the `start-master.sh` 
and `start-worker.sh
spark://boss:7077 -c 1 -m 512M` commands respectively (you'll need to specify
the full path to these .sh scripts). These scripts launch the Spark
boss and workers in the background and then exit. Make sure that the containers
do not exit along with the script and instead keep running until manually stopped.

You should then be able to use the `docker-compose.yml` we proved to
run `docker compose up -d`.  Wait a bit and make sure all containers
are still running.  If some are starting up and then exiting, troubleshoot
the reason before proceeding.

## Data Setup

### Virtual Machine

The Docker Compose setup maps a	`nb` directory into your Jupyter
container.  Within `nb`, you need to create a subdirectory called
`data` and fill it with some CSVs you'll use for the project.

You can	run the	following on your VM (not in any container):

```
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/arid2017_to_lei_xref_csv.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/code_sheets.zip
mkdir -p nb/data
unzip -o hdma-wi-2021.zip -d nb/data
unzip -o arid2017_to_lei_xref_csv.zip -d nb/data
unzip -o code_sheets.zip -d nb/data
```

You'll probably	need to	change some permissions	(`chmod`) or run as
root (`sudo su`) to be able to do this.
