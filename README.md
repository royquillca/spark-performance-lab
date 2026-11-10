
# Spark Performance Lab

A local Apache Spark performance optimization laboratory using PySpark and Docker Compose.

## 1. Objective

Build an ETL pipeline and understand how Apache Spark executes and optimizes distributed workloads.

### Topics
- Spark architecture: Driver, Master, Workers, Executors
- Logical and physical execution plans
- `explain("formatted")`
- Shuffle operations and partitioning
- Broadcast Hash Join vs Sort Merge Join
- Adaptive Query Execution (AQE)
- Data skew and performance tuning
- Spark UI and execution metrics

## 2. Tech Stack

- Python / PySpark
- Apache Spark
- Docker Compose
- Parquet
- Git

## 3. Local Environment

- OS: Windows 11
- Docker Desktop
- Available Docker RAM: 8 GB
- Spark Workers: 2
- CPU: 1 core per Worker
- Worker Memory: 1 GB each
- Driver Memory: 1 GB
- Initial Dataset: 100,000 orders

## 4. Architecture

Spark Standalone Cluster:

- Spark Master: coordinates cluster resources
- Spark Workers: provide compute resources
- Spark Driver: runs the application and schedules tasks
- Executors: execute tasks on Workers

## 5. Implementation Progress

### Stage 1: Project Setup

- [x] Initialize Git repository
- [x] Create project structure
- [x] Create README.md
- [ ] Configure Docker Compose
- [ ] Start Spark cluster
- [ ] Verify Spark Master and Workers

### Stage 2: Data Pipeline

- [ ] Generate synthetic data
- [ ] Build baseline ETL
- [ ] Inspect physical execution plan

### Stage 3: Performance Optimization

- [ ] Benchmark execution time
- [ ] Optimize joins
- [ ] Optimize partitions and shuffle
- [ ] Test AQE and data skew
- [ ] Compare performance results


## 6. Docker Infrastructure

### Step 1: Spark Master

The Spark Master manages cluster resources and registers
available Spark Workers.

We use the official Apache Spark Docker image to avoid
installing Spark and Java manually.

**Configuration:**

- Spark version: 4.3.0
- Deployment mode: Standalone
- Master hostname: spark-master
- Master port: 7077
- Web UI port: 8080

**Start the Master:**

```bash
docker compose up -d spark-master
```

**Verify the container:**

```bash
docker compose ps
docker compose logs spark-master
```

**Access the Master UI:**

http://localhost:8080

**Expected result:**

- Spark Master is running.
- Web UI is accessible.
- No Workers are registered yet.

**Stop the cluster:**

```bash
docker compose down
```


### Troubleshooting: Port Already in Use

If port 8080 is already occupied by another application,
Docker cannot expose the Spark Master UI.

Solution: change the host port mapping in docker-compose.yml.

```yaml
ports:
  - "127.0.0.1:8081:8080"
```

The Spark Master still listens on port 8080 inside
the container, but the UI is accessible from:

http://localhost:8081

Apply the configuration:

```bash
docker compose up -d spark-master
```

