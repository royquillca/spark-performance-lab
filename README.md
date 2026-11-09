
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
