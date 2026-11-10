
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

### Step 2: Spark Worker 1

A Spark Worker provides CPU and memory resources to
run Spark Executors.

**Configuration:**

- Worker hostname: spark-worker-1
- CPU cores: 1
- Spark memory: 1 GB
- Master URL: spark://spark-master:7077

**Start the Worker:**

```bash
docker compose up -d spark-worker-1
```

**Verify the Worker:**

```bash
docker compose ps
docker compose logs --tail 30 spark-worker-1
```

Open http://localhost:8081 and verify:

- Workers Alive: 1
- Total Cores: 1
- Total Worker Memory: 1 GiB

**Important:** Worker memory represents resources available
to Spark Executors, not the container's total memory usage.


### Step 3: Spark Worker 2

A second Spark Worker increases the cluster's available
CPU and memory resources, enabling parallel task execution
across multiple Workers.

**Configuration:**

- Worker hostname: spark-worker-2
- CPU cores: 1
- Spark memory: 1 GB
- Master URL: spark://spark-master:7077

Both Workers use the same Spark image and resource
configuration.

**Start the second Worker:**

```bash
docker compose up -d spark-worker-2
```

**Verify the cluster:**

```bash
docker compose ps
docker compose logs --tail 30 spark-worker-2
```

**Expected Spark Master UI:**

- Workers Alive: 2
- Total Cores: 2
- Total Memory: 2 GiB
- Running Applications: 0

**Key Learning:**

Workers provide resources to Spark Executors.
Adding Workers increases available parallelism, but
performance also depends on task partitioning,
shuffle operations, and workload characteristics.


## 7. First PySpark Application

### Objective

Submit a PySpark application to the standalone Spark
cluster and inspect its physical execution plan.

### Implementation

The `src/first_job.py` script:

1. Creates a SparkSession.
2. Generates 100,000 synthetic records.
3. Distributes the input across two partitions.
4. Groups records into four categories.
5. Displays the physical execution plan.
6. Executes an action and displays the results.

### Docker Bind Mount

The `src` directory is mounted into the Spark Master
container to execute local Python scripts without
rebuilding the Docker image.

### Run

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --executor-memory 1g \
  --executor-cores 1 \
  /opt/spark/work-dir/src/first_job.py
```

### Expected Results

- Application completes successfully.
- Output contains four categories.
- Each category contains 25,000 records.
- The physical execution plan is displayed.
- Spark executors use the standalone cluster resources.

### Key Concepts

- SparkSession
- Driver and Executors
- DataFrame transformations
- Lazy evaluation
- Actions
- Physical execution plans


### Troubleshooting: Git Bash Path Conversion

Git Bash on Windows may automatically convert Linux paths
into Windows paths when executing Docker commands.

For example:

`/opt/spark/bin/spark-submit`

may be converted to:

`C:/Program Files/Git/opt/spark/bin/spark-submit`

To prevent this issue, disable MSYS path conversion
for the command:

```bash
MSYS_NO_PATHCONV=1 docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --executor-memory 1g \
  --executor-cores 1 \
  /opt/spark/work-dir/src/first_job.py
```

This setting applies only to the current command.


### First Job Execution Results

**Status:** Successful

The first PySpark application processed 100,000 records
using two Spark Executors across two Workers.

**Observed execution plan:**

```text
AdaptiveSparkPlan
+- HashAggregate
   +- Exchange
      +- HashAggregate
         +- Project
            +- Range
```

**Key findings:**

- Spark generated two initial input partitions.
- Both Workers participated in task execution.
- Partial aggregation reduced data before the shuffle.
- Exchange redistributed data by category.
- AQE was enabled during execution.
- All four categories contained 25,000 records.
- The application finished successfully.

**Optimization observations:**

The physical plan initially specified 200 shuffle
partitions, but AQE can coalesce shuffle partitions
at runtime.

Future experiments will compare execution with
different shuffle partition settings and AQE enabled
or disabled.
