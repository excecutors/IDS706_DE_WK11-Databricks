# Week 11 — PySpark Data Processing (NYC FHVHV Trips, Jan–Mar 2025)

This project builds a distributed data pipeline in PySpark on the **NYC For-Hire Vehicle High Volume (FHVHV: Uber/Lyft)** trip dataset (Jan–Mar 2025). It demonstrates **lazy vs eager execution**, **transformations and actions**, **SQL queries**, **joins**, **partitioned writes**, **predicate pushdown**, **broadcast joins**, and **AQE**.

---

## 1) Datasets & Sources

* **FHVHV Trip Records (Jan–Mar 2025)** — Parquet files

  * `fhvhv_tripdata_2025-01.parquet`
  * `fhvhv_tripdata_2025-02.parquet`
  * `fhvhv_tripdata_2025-03.parquet`
  * Source: NYC TLC Trip Record Data (public)
* **Taxi Zone Lookup** (CSV) — optional enrichment for joins

  * `taxi_zone_lookup.csv` with columns `LocationID`, `Borough`, `Zone`

### Storage Layout (Unity Catalog Volumes)

* **Input Volume**: `/Volumes/main/default/nyc_fhvhv/`
* **Output Volume**: `/Volumes/main/default/nyc_fhvhv_out/`
* **Lookup**: `/Volumes/main/default/nyc_fhvhv_out/lookup/taxi_zone_lookup.csv`

---

## 2) How to Run

1. Create catalog/schema/volumes:

   ```sql
   CREATE CATALOG IF NOT EXISTS main;
   CREATE SCHEMA  IF NOT EXISTS main.default;
   CREATE VOLUME  IF NOT EXISTS main.default.nyc_fhvhv;
   CREATE VOLUME  IF NOT EXISTS main.default.nyc_fhvhv_out;
   ```
2. Copy the three Parquet files from the notebook workspace folder into the input **Volume**:

   ```python
   src_base = "dbfs:/Workspace/Users/<your_email>/Week 11 PySpark Data Processing/"
   dst_base = "/Volumes/main/default/nyc_fhvhv"
   dbutils.fs.mkdirs(dst_base)
   for m in ["01","02","03"]:
       dbutils.fs.cp(f"{src_base}fhvhv_tripdata_2025-{m}.parquet", f"{dst_base}/fhvhv_tripdata_2025-{m}.parquet", recurse=False)
   display(dbutils.fs.ls(dst_base))
   ```
3. Read all months:

   ```python
   src_df_path = "/Volumes/main/default/nyc_fhvhv"
   fhvhv = spark.read.parquet(f"{src_df_path}/fhvhv_tripdata_2025-*.parquet")
   fhvhv.printSchema()
   ```
4. Run the **transformations, filters, KPIs, SQL** and **writes** as in the notebook code.

> **Note**: The notebook uses Spark SQL Adaptive Execution (AQE) and Photon on Databricks (as reflected in the plans). Adjust `spark.sql.shuffle.partitions` to cluster size.

---

## 3) Pipeline Overview

### Transformations (withColumn)

* Parse timestamps: `pickup_ts`, `dropoff_ts`
* Derive partitions: `year`, `month`
* Compute `trip_minutes`, `speed_mph`
* Normalize monetary fields with `coalesce` and build **`revenue`**
* Compute `tip_rate = tips / fare` (guarded for zero fare)

### Early Filters (Predicate Pushdown)

* Remove null `PULocationID`/`DOLocationID`
* Positive `trip_miles` and `trip_time`
* Reasonable `speed_mph` in `[1, 90]`

### Column Pruning

* Keep only columns required for downstream operations to reduce scan/shuffle footprint.

### GroupBy Aggregations

* **Monthly KPIs** per `(year, month)`:

  * `trips`, `avg_speed`, `revenue_sum`, `avg_tip_rate`

### Two Spark SQL Queries

* **SQL A**: Monthly KPIs with explicit `WHERE speed_mph BETWEEN 1 AND 90`
* **SQL B**: Top `PULocationID` by trip count and `revenue_sum` for Jan–Mar 2025

### Join (Enrichment)

* Broadcast join with **Taxi Zone Lookup** to attach `PU_Borough`/`DO_Borough`
* Aggregation of `rev` and `trips` by `PU_Borough` (`top_borough`)

### Writes (Parquet)

* Partitioned output for `fhvhv_tr` by `year, month`
* Outputs:

  * `/nyc_fhvhv_out/fhvhv_tr_partitioned/`
  * `/nyc_fhvhv_out/kpi_month/`
  * `/nyc_fhvhv_out/sql_a/`, `/nyc_fhvhv_out/sql_b/`
  * `/nyc_fhvhv_out/top_borough/`

---

## 4) Optimization Strategies Used

* **Early Filters**: Applied before heavy shuffles; push down into Parquet scans.
* **Column Pruning**: `select(...)` narrows columns prior to shuffle/aggregation.
* **Partitioned Writes**: `partitionBy("year","month")` for downstream pruning.
* **Broadcast Join**: `taxi_zone_lookup` is tiny → `broadcast(zones)` to avoid big shuffles.
* **AQE**: `spark.sql.adaptive.enabled=true` (Databricks) → coalesced shuffles, skew mitigation.
* **File Sizing**: `spark.sql.files.maxPartitionBytes=128MB` (example) to balance parallelism.

---

## 5) Performance Analysis (Execution Plans)

Since the Databricks Free Edition does not provide full Spark UI access,
execution plans were captured directly from the `.explain(mode="formatted")`
outputs of three representative queries:

| File | Query / Stage | Description |
|------|----------------|-------------|
| `docs/explain_kpi_month.png` | **kpi_month** | Monthly KPI aggregation (groupBy and aggregation stages). Shows `PhotonGroupingAgg`, `PhotonProject`, and `AdaptiveSparkPlan`. Demonstrates vectorized aggregation and column pruning. |
| `docs/explain_sql_a.png` | **sql_a** | SQL query computing monthly KPIs with an explicit filter on valid speed ranges. Shows `PhotonScan parquet` with `RequiredDataFilters`, confirming **predicate pushdown** and column pruning. |
| `docs/explain_top_borough.png` | **top_borough** | Join and aggregation by pickup borough using a broadcast lookup table. Shows `PhotonBroadcastHashJoin` and `AdaptiveSparkPlan`, confirming Spark automatically broadcasted the small lookup dataset. |

Each `.explain()` output includes a **"== Physical Plan =="** section followed by
`== Photon Explanation == The query is fully supported by Photon.`, verifying that
Databricks executed the entire pipeline using **Photon’s columnar optimizer**.

### Key Optimizations Observed

* **Predicate Pushdown:**  
  `PhotonScan parquet` blocks include `RequiredDataFilters`, proving that filters on trip distance, duration, and speed were applied **before** shuffles to minimize I/O.

* **Column Pruning:**  
  `PhotonProject` stages show only the required fields (`trip_miles`, `trip_time`, `fare`, `tips`, etc.), reducing serialization overhead.

* **Aggregation Optimization:**  
  `PhotonGroupingAgg` appears in both `kpi_month` and `sql_a`, showing partial → shuffle → final aggregation pattern for efficient vectorized processing.

* **Broadcast Join:**  
  `top_borough` contains a `PhotonBroadcastHashJoin LeftOuter`, confirming that the small taxi-zone lookup file was broadcast to avoid a large shuffle on the main dataset.

* **Adaptive Query Execution (AQE):**  
  `AdaptiveSparkPlan` nodes appear in all plans, showing dynamic shuffle coalescing and skew handling during runtime.

### Bottlenecks & Mitigations

* **Shuffle Overhead:** inevitable for groupBy stages; mitigated by AQE.  
* **Skew Handling:** AQE automatically merged skewed partitions.  
* **Output Parallelism:** `coalesce(1)` used for readability only; in production, parallel partitioned writes should be preserved.

### KPI Summary (Jan–Mar 2025)

| Month | Trips | Avg Speed (mph) | Total Revenue (USD) |
|--------|-------|----------------|--------------------|
| Jan 2025 | 20,400,288 | 13.94 | ≈ 6.26 × 10⁸ |
| Feb 2025 | 19,334,052 | 13.69 | ≈ 6.09 × 10⁸ |
| Mar 2025 | 20,531,303 | 13.80 | ≈ 7.09 × 10⁸ |

All three plans confirm full **Photon execution** with efficient columnar scanning,
broadcast joins, and AQE. The screenshots in the `docs/` folder serve as the
“Execution Plan and Spark UI Screenshots” deliverables for this submission.

---

## 6) Actions vs. Transformations

* **Transformations (lazy)**: `filter`, `select`, `withColumn` — build the logical plan only.
* **Actions (eager)**: `count()`, `show()`, `write` — **trigger** execution of the plan.

Demo:

```python
lazy_df = with_zones.filter((F.col("year")==2025) & (F.col("month")==1)).select("pickup_ts","revenue")
print("Actions vs Transformations — count():", lazy_df.count())
```

---

## 7) Repository Layout (suggested)

```
IDS706_DE_WK11/
├── README.md
├── Week11_PySpark_FHVHV.ipynb
└── docs/
    ├── explain_kpi_month.png
    ├── explain_sql_a.png
    └── explain_top_borough.png
```

---

## 8) Grading Checklist (Rubric Mapping)

* **Dataset Selection & Loading**: 3× Parquet files (1GB+ combined) ✅
* **Transformations**: withColumn derivations; 2+ filters; column pruning ✅
* **Join OR Complex Aggregation**: Broadcast join to zone lookup ✅
* **groupBy Aggregations**: Monthly KPIs ✅
* **2+ SQL Queries**: `sql_a`, `sql_b` ✅
* **Optimization**: Early filters, partitioned writes, broadcast join, AQE, file sizing ✅
* **Output Writing**: Parquet outputs in Volume ✅
* **Execution Plans / UI**: `.explain()` + Query Details screenshots ✅
* **Performance Analysis**: Section 5 above ✅
* **Actions vs Transformations**: Section 7 demo ✅

---

## 9) Notes

* Monetary fields are `coalesce`d to avoid null arithmetic; `revenue` includes fare, tips, tolls, taxes, surcharges, and fees.
* `speed_mph` is computed from `trip_miles` and `trip_time` (seconds); extreme values are filtered.
* The join uses **PU** and **DO** `LocationID`s to attach borough/zone names.
* Photon support is visible in `.explain()` output; enable AQE for best shuffle handling.
