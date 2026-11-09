# Distributed NLP Analysis of COVID-19 Papers with Dask

<p align="center">
  <img src="docs/logo.jpeg" alt="Project Cover" width="100%" />
</p>

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Dask](https://img.shields.io/badge/Distributed-Dask-orange.svg)](https://dask.org/)
[![Course](https://img.shields.io/badge/UniPD-MAPD--B-red.svg)](https://www.unipd.it/)

An end-to-end distributed natural language processing (NLP) and bibliometric pipeline analyzing scientific publications from the **CORD-19 (COVID-19 Open Research Dataset)**.

Developed as the final project for the **Management and Analysis of Physics Datasets (MAPD - Module B)** course (A.Y. 2020/2021) at the **University of Padua**, Master's Degree in Physics of Data.

---

## 📌 Project Overview

Processing large-scale corpus collections (>20 GB of nested JSON scientific papers) quickly exceeds the memory capacity of individual laptops. In this project, we designed and deployed an ad-hoc **multi-node LAN compute cluster** using **Dask Distributed** and **NFS (Network File System)** to execute:

1. **MapReduce Word Counter**: Distributed tokenization, stop-word filtering, and parallel frequency reduction using `dask.bag`.
2. **Bibliometric & Metadata Extraction**: Author affiliations and country attributions extracted from nested JSON structures into `dask.dataframe` for distributed relational aggregation.
3. **Distributed Semantic Embeddings**: Loading a pre-trained **FastText** corpus embedding (~2 GB) partitioned across cluster nodes to compute centroid vector representations of research paper titles.
4. **Paper Title Similarity Matrix**: Distributed pairwise cosine similarity using `scikit-learn` and `joblib` backed by Dask workers to identify related publications and duplicated preprints.
5. **Cluster Profiling & Benchmark Analysis**: Scalability analysis evaluating the impact of worker count, process vs. thread parallelism, partition granularity (`npartitions`), and network overhead.

---

## 🏗️ Architecture & Cluster Setup

The cluster was constructed across personal machines connected over a local area network (LAN):

```text
                      +-----------------------------+
                      |   Dask Scheduler + Master   |
                      |   192.168.1.125:8786        |
                      +--------------+--------------+
                                     |
             +-----------------------+-----------------------+
             |                                               |
             v                                               v
+-------------------------+                     +-------------------------+
|     Worker Node 1       |                     |     Worker Node 2       |
|  (Cores, RAM, Storage)  |                     |  (Cores, RAM, Storage)  |
+------------+------------+                     +------------+------------+
             |                                               |
             +-----------------------+-----------------------+
                                     |
                                     v
                      +-----------------------------+
                      |    Shared NFS Storage       |
                      |  /mnt/nfs/covid_distributed |
                      +-----------------------------+
```

* **Network File System (NFS)**: Shared data directory accessible by all worker machines.
* **Dask Scheduler**: Orchestrates task graph construction, lazy evaluation (DAGs), and work-stealing across nodes.
* **Monitoring**: Real-time performance tracking via the Dask Web Diagnostic Dashboard.

> For detailed NFS mounting and worker initialization steps, see [docs/cluster_setup.md](docs/cluster_setup.md).

---

## 🔬 Pipeline & Methodology

### 1. Data Ingestion & Preprocessing
Raw publications from CORD-19 contain multi-line nested JSON structures. We compacted these into single-line JSON representations using `jq -c .` via [`scripts/preprocess_json.sh`](scripts/preprocess_json.sh), enabling efficient streaming with `dask.bag.read_text`.

### 2. Distributed Word Count (MapReduce)
* Documents are tokenized with NLTK, stripping standard English stop words and domain-specific noise (`et`, `al`, etc.).
* Words are mapped into `(word, 1)` tuples and aggregated through a two-stage associative reduction (`binop` fold).

### 3. Country & Institutional Attribution
* Nested metadata structures are denormalized and projected into a typed `dask.dataframe`.
* Distributed `groupby` aggregations rank the leading national and academic contributors to early COVID-19 research.

### 4. FastText Semantic Title Embeddings
* Pre-trained FastText vectors (1 million words trained on Wikipedia, UMBC, and statmt.org news) are ingested into partitioned bags.
* Title embeddings are constructed by vector-summing normalized word vectors for all tokens present in the vocabulary.

### 5. Pairwise Cosine Similarity
* Titles and embeddings are normalized and cross-compared using parallel scikit-learn distance metrics:
  $$\text{Cosine Similarity}(u, v) = \frac{u \cdot v}{\|u\|_2 \|v\|_2}$$
* Off-diagonal maximums successfully identify identical preprints submitted across different platforms as well as thematic clusters in clinical palliative care and viral response.

---

## 📊 Benchmark & Profiling Insights

* **Worker Configuration**: Balanced multi-threaded workers (`--nthreads 2`) demonstrated higher throughput than single-threaded configurations, though memory limits must be constrained to prevent OS worker termination.
* **Partition Size Impact**:
  * **Dask Bags**: Computation times for unstructured operations (word counting) remained relatively insensitive to partition count.
  * **Dask DataFrames**: Execution times exhibited a parabolic curve: throughput peaked when partition count closely matched the file chunk size, avoiding unnecessary task scheduling overhead.
  * **Memory Constraints**: Lazy evaluation enabled executing workflows on datasets far exceeding the memory capacity of any individual node.

---

## 📂 Repository Structure

```text
├── README.md                          # Project documentation and summary
├── environment.yml                    # Clean Conda environment definition
├── requirements.txt                   # Pip package dependencies
├── .gitignore                         # Exclusions for cache and large datasets
├── docs/
│   ├── MAPD-B_Gr04-Presentation.pdf  # Project defense presentation slides
│   ├── cluster_setup.md              # NFS and Dask worker connection guide
│   ├── logo.jpeg                     # Project cover image
│   └── requirements_legacy.yml       # Raw conda freeze from original cluster
├── notebooks/
│   ├── 01_dask_bag_prototyping.ipynb # Initial experiments with Dask Bags
│   └── 02_distributed_covid_nlp.ipynb# Main end-to-end distributed pipeline
└── scripts/
    └── preprocess_json.sh             # Batch JSON compaction utility with jq
```

---

## 🚀 Getting Started

### Prerequisites
* Python 3.8+
* Conda or Mamba

### Environment Setup

```bash
# Clone the repository
git clone https://github.com/francescozambelli/4_FAKENEWS.git
cd 4_FAKENEWS

# Create conda environment
conda env create -f environment.yml
conda activate covid-nlp-dask

# Download required NLTK resources
python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"
```

### Running the Analysis
1. Start the Dask scheduler and worker nodes as outlined in [`docs/cluster_setup.md`](docs/cluster_setup.md).
2. Launch Jupyter Lab:
   ```bash
   jupyter lab
   ```
3. Open [`notebooks/02_distributed_covid_nlp.ipynb`](notebooks/02_distributed_covid_nlp.ipynb), set your scheduler IP address (`tcp://<SCHEDULER_IP>:8786`), and run the cells.

---

## 👥 Authors (Group 04)

* **Noemi Manara**
* **Giuseppe Simionato**
* **Nicholas Sinigaglia**
* **Francesco Zambelli**

*Master's in Physics of Data — Università degli Studi di Padova (A.Y. 2020/2021)*
