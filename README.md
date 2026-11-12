
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
- [x] Configure Docker Compose
- [x] Start Spark cluster
- [x] Verify Spark Master and Workers

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


## 8. Spark Observability

### Step 1: Enable Spark Event Logging

Spark Event Logs record application execution events and
allow Spark History Server to reconstruct completed jobs.

**Configuration:**

- Event logging: enabled
- Event log directory: `/opt/spark/events`
- Host directory: `./events`

The events directory is mounted into the Spark container
to persist logs across container restarts.

**Spark submit options:**

```text
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=file:///opt/spark/events
```

**Expected result:**

- The Spark application completes successfully.
- Event log files are created in `./events`.
- Logs can later be analyzed using Spark History Server.

**Next step:** Configure Spark History Server.


### Step 2: Spark History Server

Spark History Server reconstructs completed Spark
applications from persisted event logs.

**Configuration:**

- Image: apache/spark:4.3.0-python3
- Service: spark-history
- Web UI: http://localhost:18080
- Event log directory: /opt/spark/events
- JVM memory: 512 MB
- Event log access: read-only

**Start the History Server:**

```bash
docker compose up -d spark-history
```

**Verify:**

```bash
docker compose ps
docker compose logs --tail 40 spark-history
```

**Expected result:**

The Spark History Server displays completed applications,
including SparkPerformanceLab-FirstJob.

**Performance analysis:**

The History Server provides access to:

- Jobs and execution duration
- Stages and task-level metrics
- SQL execution plans
- Shuffle read and write metrics
- Executor resource usage

**Key learning:**

Event Logs preserve application execution information
after the Spark Driver stops.


## 9. Spark Performance Optimization Guide

### 9.1 Optimization Workflow

Follow a measurement-driven approach:

1. Understand the input datasets and workload.
2. Establish a reproducible performance baseline.
3. Inspect the physical execution plan.
4. Analyze Jobs, Stages, Tasks, and Executors.
5. Identify the primary performance bottleneck.
6. Apply one optimization at a time.
7. Rerun the same workload.
8. Compare performance metrics.
9. Validate output correctness.
10. Document findings and trade-offs.

### 9.2 Important Metrics

| Metric | Purpose |
|---|---|
| Job Duration | Measures job execution time |
| Task Duration | Identifies slow or uneven tasks |
| Shuffle Read | Measures data read from shuffle |
| Shuffle Write | Measures data written for shuffle |
| Spill to Disk | Indicates intermediate data spilled to disk |
| JVM GC Time | Measures garbage collection overhead |
| Input Size | Measures data read by the workload |
| Task Count | Helps evaluate execution parallelism |

### 9.3 Physical Plan Operators

| Operator | Meaning |
|---|---|
| Range | Generates a sequence of records |
| Project | Computes or selects columns |
| Filter | Applies row filtering |
| HashAggregate | Performs hash-based aggregation |
| Exchange | Redistributes data across partitions |
| SortMergeJoin | Joins sorted, partitioned datasets |
| BroadcastHashJoin | Joins using a replicated small dataset |
| AdaptiveSparkPlan | Enables runtime plan optimization |

### 9.4 Benchmark Rules

- Use identical inputs for comparisons.
- Keep cluster resources consistent.
- Measure equivalent application workloads.
- Separate application startup from job execution time.
- Perform warm-up runs when appropriate.
- Repeat measurements and compare median runtimes.
- Record execution plans and shuffle metrics.
- Verify output correctness after every optimization.
- Change one optimization variable at a time.

### 9.5 Experiment Roadmap

- [x] Start Spark Standalone cluster
- [x] Execute first distributed PySpark job
- [x] Enable Spark Event Logs
- [x] Start Spark History Server
- [ ] Analyze Jobs, Stages, and Tasks
- [ ] Establish an ETL performance baseline
- [ ] Optimize shuffle partitions
- [ ] Compare Broadcast Hash Join and Sort Merge Join
- [ ] Investigate data skew
- [ ] Evaluate Adaptive Query Execution
- [ ] Optimize Parquet reads and writes
- [ ] Benchmark and document improvements


### First Job: Execution Analysis

The first application generated two Spark Jobs.

**Job 0:**
- Duration: approximately 5 seconds
- Executed tasks: 2
- Processed two initial input partitions

**Job 1:**
- Duration: approximately 1 second
- Executed tasks: 1
- One previous stage was skipped because its
  results were already available

**Key observations:**

- Spark can execute multiple Jobs for a single DataFrame action.
- Each input partition can produce an independent task.
- Spark can reuse completed shuffle stages.
- AQE may reduce the number of shuffle partitions.
- Application uptime is different from actual Job duration.

**Next investigation:**

Inspect task duration, shuffle metrics, and executor
distribution in the Stages tab.


### First Performance Diagnosis

**Application:** SparkPerformanceLab-FirstJob

#### Stage Metrics

| Metric | Stage 0 | Stage 2 |
|---|---|---|
| Duration | 4 s | 0.9 s |
| Tasks | 2 | 1 |
| Shuffle Write | 496 B | N/A |
| Shuffle Read | N/A | 496 B |

#### Stage 0 Task Metrics

- Input records: 50,000 per task
- Task duration: 2–3 seconds
- Task deserialization: approximately 2 seconds
- GC time: 37–83 milliseconds
- Shuffle write: 248 bytes per task
- Peak execution memory: 256 KiB

#### Findings

1. Input data was evenly distributed between two tasks.
2. Partial aggregation significantly reduced shuffle volume.
3. Task preparation overhead was substantial relative to
   the small workload.
4. Garbage collection overhead was relatively low.
5. No significant data skew was observed.
6. No clear performance bottleneck requires optimization yet.

#### Conclusion

This workload is too small to demonstrate many meaningful
Spark optimization improvements.

The next experiment should use a larger dataset and
more expensive operations such as joins and aggregations.



### 9.6 Explain Plans vs Spark UI

`DataFrame.explain()` describes how Spark plans to execute
a query, while Spark UI provides runtime execution metrics.

**Explain plans help identify:**
- Physical join strategies
- Shuffle boundaries (`Exchange`)
- Aggregation operators
- Filter and projection pushdown
- Logical and physical query optimizations

**Spark UI helps measure:**
- Job and stage duration
- Task execution distribution
- Shuffle read and write
- Memory spills and garbage collection
- Executor utilization

Both tools should be used together.

### 9.7 Production Performance Investigation

1. Identify the performance regression or SLA violation.
2. Compare current and historical workload characteristics.
3. Locate expensive jobs, stages, and tasks.
4. Inspect SQL physical execution plans.
5. Identify likely bottlenecks.
6. Form a measurable optimization hypothesis.
7. Apply one controlled change.
8. Benchmark against the original execution.
9. Validate correctness, stability, and resource cost.
10. Document the results and monitor after deployment.

**Principle:**

Measure first, understand the execution plan,
optimize the bottleneck, and verify the improvement.


## 10. Retail Sales Analytics: Star Schema

### 10.1 Business Scenario

A retail company needs to analyze sales performance
across products, categories, stores, and regions.

The objective is to build a reproducible analytical
workload to investigate Apache Spark execution plans
and performance optimization strategies.

This project focuses on distributed data processing,
not on creating a complete retail transaction system.

### 10.2 Dimensional Modeling Fundamentals

A Star Schema is a dimensional data model with a
central fact table connected to descriptive dimension
tables.

**Fact Table**

A fact table stores measurable business events.

Our `fact_sales` table represents individual sales.

Its grain, or level of detail, is one row per sale.

Columns:
- sale_id: logical primary key
- product_id: product dimension reference
- store_id: store dimension reference
- sale_date: date of the sale
- quantity: number of units sold
- unit_price: price per unit at the time of sale

Revenue is calculated as:

    revenue = quantity * unit_price

**Dimension Tables**

Dimension tables provide descriptive context
for business events.

`dim_product`:
- product_id
- category
- brand

`dim_store`:
- store_id
- region
- city

**Keys**

PK (Primary Key): identifies a record within a table.

FK (Foreign Key): references a record in another table.

In this project, these relationships are logical.
Parquet files do not automatically enforce primary key
or foreign key constraints.

### 10.3 Why Only Three Tables?

The laboratory uses the minimum meaningful star schema
for studying multiple joins and aggregations.

We intentionally exclude customer, payment, inventory,
and calendar dimensions because they are not required
for the current analytical workload.

### 10.4 Dataset Sizes

| Dataset | Initial Rows |
|---|---:|
| fact_sales | 100,000 |
| dim_product | 1,000 |
| dim_store | 40 |

These volumes establish a functional baseline.

Future experiments may increase the fact table size
to make performance differences more observable.

### 10.5 Why Apache Parquet?

Apache Parquet is a column-oriented storage format
commonly used in analytical data processing.

Unlike traditional row-oriented storage, a columnar
format allows analytical engines to read only the
columns required by a query, when supported.

We will investigate:

- Column pruning
- Predicate pushdown
- Compression
- File sizes
- Partitioning
- Read performance

### 10.6 Shared Storage in Docker Compose

The dataset directory is mounted into the Spark
Master and both Spark Workers:

    ./data:/opt/spark/work-dir/data

This provides a consistent filesystem path across
the three containers.

The Spark Driver plans the workload, while Executors
running on Workers read and process the data.

This local bind mount is suitable for experimentation
but is not a replacement for production distributed
storage.

### 10.7 Data Generation Strategy

The `src/generate_data.py` script generates synthetic
datasets using native PySpark DataFrame transformations.

The generator is deterministic: the same configuration
produces the same logical records.

No external data generation libraries are required.

**Important design decisions:**

1. Generate surrogate-style numeric identifiers.
2. Use deterministic expressions for attributes.
3. Avoid unnecessary Python User-Defined Functions.
4. Store datasets in Parquet format.
5. Use overwrite mode for reproducibility.
6. Start with small data volumes.
7. Introduce controlled skew in future experiments.

Synthetic distributions are not assumed to match
real-world retail behavior.

### 10.8 Generate the Datasets

Execute from Git Bash on Windows:

```bash
MSYS_NO_PATHCONV=1 docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///opt/spark/events \
  --executor-memory 1g \
  --executor-cores 1 \
  /opt/spark/work-dir/src/generate_data.py \
  --rows 100000
```

`MSYS_NO_PATHCONV=1` prevents Git Bash from converting
Linux container paths into Windows paths.

### 10.9 Validation Checklist

- [ ] All three datasets are generated successfully.
- [ ] Parquet files exist in the data directory.
- [ ] Both Workers can access the shared data directory.
- [ ] Data generation appears in Spark History Server.
- [ ] No failed tasks or application errors occur.

### 10.10 Next Experiment

Build the baseline analytical ETL pipeline.

ETL stands for Extract, Transform, Load.

The pipeline will:

1. Read fact and dimension datasets from Parquet.
2. Join sales with products and stores.
3. Aggregate revenue by region and category.
4. Inspect the physical plan with explain().
5. Execute and measure the workload.
6. Analyze Spark SQL, Jobs, Stages, and Tasks.
7. Save baseline metrics for later comparisons.

The initial baseline will use Spark's default optimizer
behavior without manually forcing a join strategy.
