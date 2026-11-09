# Cluster & NFS Setup Guide

This document describes how to set up the local area network (LAN) distributed cluster using Network File System (NFS) and Dask.

---

## 1. Prerequisites on Worker Nodes

Each worker machine on the LAN needs NFS client utilities and the conda environment.

### One-Time Setup

```bash
# 1. Install NFS client package (Debian/Ubuntu)
sudo apt update
sudo apt install -y nfs-common

# 2. Create the local mount point
sudo mkdir -p /mnt/nfs/covid_distributed

# 3. Create conda environment from configuration
conda env create -f environment.yml
```

---

## 2. Worker Node Execution

On every cluster session:

```bash
# 1. Mount the shared NFS directory from the master/host node (adjust IP as needed)
sudo mount 192.168.1.125:/home/noemi/covid_distributed /mnt/nfs/covid_distributed

# 2. Activate the conda environment
conda activate covid-nlp-dask

# 3. Connect as a Dask worker to the scheduler
dask-worker tcp://192.168.1.125:8786 --nthreads 2 --memory-limit 4GB
```

---

## 3. Master / Host Node Setup (Reference)

On the machine hosting the dataset and scheduler:

```bash
# 1. Export directory via /etc/exports
# /home/noemi/covid_distributed 192.168.1.0/24(rw,sync,no_subtree_check)
sudo exportfs -a

# 2. Start Dask Scheduler
dask-scheduler --port 8786 --dashboard-address 8787
```

You can view the real-time cluster monitoring dashboard at:
`http://<MASTER_IP>:8787/status`
