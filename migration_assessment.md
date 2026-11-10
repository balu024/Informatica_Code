# Informatica PowerCenter → Databricks Migration Assessment

**Scope:** `wf_Fact_Monthly_Acct_insupd.XML` and `wf_Fact_Sales_Activity_insupd.XML` (repository root)
**Status:** Assessment only. No code generated. No business logic assumed beyond what is literally in the XML.

---

## 1. Method

Both files were parsed programmatically (Python `xml.etree`). The parser walked every `FOLDER`, `SOURCE`, `TARGET`, `TRANSFORMATION`, `MAPPLET`, `SHORTCUT`, `MAPPING` (`INSTANCE`, `CONNECTOR`, `TARGETLOADORDER`, `MAPPINGVARIABLE`), `SESSION` (`SESSTRANSFORMATIONINST`, `SESSIONEXTENSION`, `CONNECTIONREFERENCE`, `SESSIONCOMPONENT`), `WORKFLOW` (`TASKINSTANCE`, `WORKFLOWLINK`, `WORKFLOWVARIABLE`, `SCHEDULER`) and `CONFIG` element. The following checks were automated:

* Every `SHORTCUT` resolves to an object in the referenced folder **inside the same export file**.
* Every mapping/mapplet `INSTANCE` resolves to a local, reusable or shortcut definition.
* Every `CONNECTOR` references an existing instance (no dangling links).
* Every session's `MAPPINGNAME` and every workflow `TASKINSTANCE` resolves to an exported object.
* Instances with no port links were identified (to separate unconnected lookups/SPs from orphans).
* All `$$` mapping parameters, `$`-session/connection variables, `:LKP.` and `:SP.` calls, SQL overrides, pre/post SQL, file references and command tasks were extracted.

Statements in this document labelled **"Observed"** are taken directly from the XML. Statements labelled **"Inference"** are interpretation and must be confirmed by the data owners.

---

## 2. Executive summary

| Item | `wf_Fact_Monthly_Acct_insupd` | `wf_Fact_Sales_Activity_insupd` |
|---|---|---|
| Repository / domain / Integration Service | `INFA_PCRS_PRD11` / `INFA_DOMAIN_PRD01` / `INFA_PCIS_ETL_GRID` | same |
| Export date (`CREATION_DATE`) | 02/06/2026 12:36:44 | 02/06/2026 12:41:06 |
| Folders | `ETL_Shared_Folder` (shared), `ETL_Facts` | same |
| Workflows | 1 | 1 |
| Worklets | 0 | 0 |
| Session task instances in workflow | 5 (5 reusable sessions) | 23 (17 reusable sessions; 4 of them reused 2–3 times) |
| Mappings | 5 | 17 |
| Mapplets | 0 | 3 (`mplt_dim_key_sk`, `mplt_PSA_Update`, `mplt_PSA_TEMP_Update`) |
| Reusable transformations | 6 | 45 (43 in shared folder, 2 in `ETL_Facts`) |
| Source definitions | 15 (all SQL Server) | 22 (20 SQL Server tables, 1 delimited flat file, 1 `Dummy`) |
| Target definitions | 5 | 12 (incl. `Dummy`) |
| Shortcuts | 26 | 47 |
| Instances inside mappings (sources + targets + transformations) | 193 | 330 |
| Email tasks | 1 (`email_FailureEmail`) | 1 (`email_FailureEmail`) |
| Command tasks | 0 | 0 standalone; 1 post-session success command |
| Broken references found | **none** | **none** |

**Verdict on completeness (Observed):** Both exports are *structurally* complete — they were exported with dependencies, so every shortcut, reusable lookup, mapplet, source definition, target definition, session and workflow link referenced is present and resolvable. All 22 mappings and all 22 sessions report `ISVALID="YES"`.

**However, the exports are not sufficient on their own to reproduce runtime behaviour.** The following are referenced but are, by design, never contained in a PowerCenter XML export (details in §8):

1. The two **parameter files** (`wf_Fact_Monthly_Acct_insupd.par`, `wf_Fact_Sales_Activity.par`) that supply 44+ `$$` parameters and every `$DBConnection_*` value. Several reused sessions behave differently *only* because of parameter-file values (e.g. `$$SALES_TXN_TYP`, `$$DB_SP_PARAM_VALUE`).
2. **Relational connection objects** (server / database names behind `$Source`, `$Target`, `$DBConnection_Source2`, `$DBConnection_TempDB`, etc.).
3. **Stored procedure bodies**: `usp_SetActivityReasonCd`, `usp_Load_Rpt_Monthly_Employee`, `usp_Collect_Table_Stats`, `usp_CreateDropTempTable`, `usp_UpdateStagingFromETL`.
4. **Database objects referenced only inside SQL overrides/lookups** (e.g. `Run_Loop_Contrl`, `BATCH_AUDIT`, `DATA_SOURCE`, `Dim_Division_Fee`, `ETL_FACT_MONTHLY_ACCT_UPD`, `ETL_UPD_STG_IFP_BIPSD_TEMP`, `STG_DIM_KEYS`, `STG_IFP_CUPAASAC`, `STG_IFP_CUPAASCN`, `STG_CAP_QUOTE_HDR`, `Dim_Employee_Role` …) — no DDL is available for these.
5. **External workflow** `wf_Fact_Sales_Activity_IND_Trueup_ins`, launched via a shell script after `s_m_Fact_Sales_Activity_reason_sk_fix_upd`.
6. **Flat files**: `Reason_Tran_Codes_Accounts.txt` (lookup) and the file named by `$InputFileName` for source `Sales_Comp_Pct`.
7. **Upstream loaders** for `DWPSA` staging tables (`STG_IFP_*`, `STG_LAW_DBGLGLT`) and the process that maintains `Run_Loop_Contrl.is_Processed`.
8. **External scheduler** configuration — both workflows have `SCHEDULETYPE="ONDEMAND"`.

No transformation type in these exports is technically impossible to implement in PySpark/Delta. The hard parts are (a) a large volume of T-SQL (single statements up to 17.7 KB) embedded in Source Qualifier / Lookup / pre-post SQL overrides, (b) stored-procedure calls with unknown logic, (c) row-order-dependent expression variables (previous-row comparisons), and (d) global temp tables (`##fsa`, `##fct_sls_acty`) used inside pre-SQL.

---

## 3. Completeness verification by object category

| Category | Present in export? | Evidence / notes |
|---|---|---|
| Workflows | Yes | 1 per file, `ISVALID=YES` |
| Workflow links & conditions | Yes | 5 links (Monthly), 23 links (Sales); all conditions captured |
| Workflow variables | Only system variables | `USERDEFINED="YES"` count = 0 in both workflows |
| Scheduler | Yes, but on-demand | `SCHEDULETYPE="ONDEMAND"`; real trigger lives outside PowerCenter |
| Worklets | N/A | none referenced |
| Sessions (reusable) | Yes | 5 + 17; all with `CONFIGREFERENCE` to `default_session_config` |
| Session-instance overrides (workflow level) | Yes | `SESSIONEXTENSION` children under `TASKINSTANCE` override connections (e.g. readers → `$DBConnection_Source2`, temp tables → `$DBConnection_TempDB`) |
| Session config object | Yes | `default_session_config` exported in each file |
| Mappings | Yes | 5 + 17, all valid |
| Mapping parameters/variables | Declared: Yes / Values: **No** | declarations present with mostly empty defaults; runtime values come from `.par` files (missing) |
| Non-reusable transformations | Yes | full ports, expressions, attributes |
| Reusable transformations | Yes | in `ETL_Shared_Folder`; all shortcuts resolve |
| Mapplets | Yes | 3 mapplets exported with internals |
| Source definitions | Yes | column-level metadata present; flat file properties present for `Sales_Comp_Pct` |
| Target definitions | Yes | column-level metadata and primary keys present |
| Lookup tables (definitions) | **Partial** | lookup ports are present, but tables used only by lookups (e.g. `Dim_Division_Fee`, `BATCH_AUDIT`, `DATA_SOURCE`, `Dim_Infopro_Employee`, `Dim_Employee`, `Dim_Contract`, `ETL_Mkt_Vertical_Xref`) have no source/target definition |
| Stored procedure definitions | **Partial** | SP transformation signature exported; SP body not exportable |
| Connection objects | **No** | only names/variables |
| Parameter files | **No** | paths only |
| Email task | Yes | `email_FailureEmail` → `$PMFailureEmailUser` (value in Integration Service config, not exported) |
| Command task / post-session command | Yes (text) | target script `start_workflow.sh` not exported |
| Flat files | **No** | definitions only |

---

## 4. Workflow inventory and dependencies

### 4.1 `wf_Fact_Monthly_Acct_insupd` (folder `ETL_Facts`)

Observed DAG (all links are sequential; every session instance has `FAIL_PARENT_IF_INSTANCE_FAILS=YES`, `TREAT_INPUTLINK_AS_AND=YES`):

```
Start
  └─[ IIF(TO_DECIMAL(TO_CHAR(SYSDATE,'DD'))=5, TRUE, FALSE) ]──►
s_m_Agg_Divisional_Avg_insupd
  └─[ Status = SUCCEEDED ]──► s_m_Fact_Monthly_Acct_Temp1_reload
        └─[ SUCCEEDED ]──► s_m_Fact_Monthly_Acct_Temp_reload
              └─[ SUCCEEDED ]──► s_m_Fact_Monthly_Acct_ins
                    └─[ SUCCEEDED ]──► s_m_Fact_Monthly_Acct_upd
```

* **Run gate (Observed):** the only link out of `Start` evaluates true only when the server date's day-of-month is 5. On any other day no session runs.
* **Failure handling (Observed):** every session attaches `email_FailureEmail` as its failure email. Recovery is disabled (`Enable Recovery = NO`).
* **Parameter file:** `$PMSourceFileDir\..\ParFiles\Facts\wf_Fact_Monthly_Acct_insupd.par` (not present).
* **Processing period (Observed):** all SQL overrides compute the load month from `MAX(Loop_Nbr) FROM Run_Loop_Contrl WHERE Table_Name='Fact_Monthly_Acct' AND is_Processed=0`. No mapping in this export writes to `Run_Loop_Contrl` (14 read references, 0 writes) — so the process that advances it is outside this export.

### 4.2 `wf_Fact_Sales_Activity_insupd` (folder `ETL_Facts`)

Observed DAG. Names are *instance* names; the reusable session they run is in brackets when different.

```
Start ──► s_m_Rpt_Monthly_Employee_SP_ins                                   (ISENABLED = NO)

Start ──► s_m_Fact_Sales_Activity_Temp_Ifp_Summarybilled_ins
      ──► s_m_Fact_Sales_Activity_Temp_SP_SummeryBilled_upd   [s_m_Fact_Sales_Activity_Temp_SP_upd]
      ──► s_m_Fact_Sales_Activity_SummeryBilled_Refresh_ins
      ──► s_m_Fact_Sales_Activity_SummeryBilled_upd
      ──► s_m_Fact_Sales_Activity_Delta_SummeryBilled_ins     [s_m_Fact_Sales_Activity_Delta_ins]
      ──► s_m_Code_Lov_LSC_insupd
      ──► s_m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins     (FAIL_PARENT_IF_INSTANCE_FAILS = NO)
      ──► s_m_Fact_Sales_Activity_Temp_SP_upd
      ──► s_m_Fact_Sales_Activity_Refresh_ins                  (FAIL_PARENT_IF_INSTANCE_FAILS = NO)
      ──► s_m_Fact_Sales_Activity_upd
      ──► s_m_Fact_Sales_Activity_Delta_ins
      ──► s_m_Fact_Sales_Activity_Temp_Lawson_Stat_ins
      ──► s_m_Fact_Sales_Activity_Temp_SP_law_upd             [s_m_Fact_Sales_Activity_Temp_SP_upd]
      ──► s_m_Fact_Sales_Activity_Refresh_law_ins             [s_m_Fact_Sales_Activity_Refresh_ins]
      ──► s_m_Fact_Sales_Activity_law_upd                     [s_m_Fact_Sales_Activity_upd]
      ──► s_m_Fact_Sales_Activity_Delta_law_ins               [s_m_Fact_Sales_Activity_Delta_ins]
            ├──► s_m_Agg_Prescriptive_Response_reload
            ├──► s_m_Dim_Sales_Comp_Pct_insupd ──► s_m_Rpt_Sales_Comp_Site_Reload
            └──► s_m_Fact_Sales_Activity_reason_sk_fix_upd
                    ├── post-session success command:
                    │     start_workflow.sh wf_Fact_Sales_Activity_IND_Trueup_ins ETL_Facts
                    └──► s_m_Agg_Sales_Activity_reload ──► s_m_Collect_Table_Stats_SP_ins
```

Every link condition except those from `Start` is `$<prev>.Status = SUCCEEDED`.

Observed facts that matter for migration:

* **Three source-system passes through the same sessions.** The chain processes, in order, the IFP "Summary Billed" feed (`STG_IFP_BIPSDCS`), the IFP "Sales Detail" feed (`STG_IFP_BIPSD`) and the Lawson GL feed (`STG_LAW_DBGLGLT`). Sessions `..._Temp_SP_upd`, `..._Refresh_ins`, `..._upd`, `..._Delta_ins` are reused across passes. The workflow-instance overrides for these reused instances are **identical** (they only re-point connections), so any behavioural difference between passes must come from parameter-file sections keyed by instance name (e.g. `$$SALES_TXN_TYP` used in `m_Fact_Sales_Activity_Delta_ins` source filter and `m_Fact_Sales_Activity_upd` SQL; `$$DB_SP_PARAM_VALUE` passed to `usp_SetActivityReasonCd`). **The parameter file is required to know what each pass does.**
* **Shared temp table across passes.** All three "Temp" mappings write to `ETL_FACT_SALES_ACTVY_TEMP` (routed to `$DBConnection_TempDB`). Only `s_m_Fact_Sales_Activity_Temp_Ifp_Summarybilled_ins` has *Truncate target table = YES*; the SalesDetail and Lawson temp loads append. Downstream mappings filter the temp table by `Sales_Trans_Type`, `DM_USER`, `DM_OPERATION_TYPE`, `Source_System`. The migration must preserve this accumulate-then-filter behaviour.
* **Non-fatal failures:** `s_m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins` and `s_m_Fact_Sales_Activity_Refresh_ins` do not fail the workflow if they fail, but the downstream link condition (`Status = SUCCEEDED`) still stops the chain at that point. Inference: the workflow can end "Succeeded" with later steps not run — confirm intended behaviour.
* **Disabled task:** `s_m_Rpt_Monthly_Employee_SP_ins` is disabled in the workflow (`ISENABLED="NO"`). Confirm whether it should be migrated.
* **External dependency:** `wf_Fact_Sales_Activity_IND_Trueup_ins` is started from the post-session success command of `s_m_Fact_Sales_Activity_reason_sk_fix_upd` (`Fail task if any command fails = NO`, recovery `Fail task and continue workflow`). That workflow is not in the repository.
* **Only one session** (`s_m_Code_Lov_LSC_insupd`) has a failure email attached in this workflow.
* **Parameter file:** `$PMSourceFileDir\..\ParFiles\Facts\wf_Fact_Sales_Activity.par` (not present).

---

## 5. Session inventory

Session-level settings are what turn a mapping into insert/update/upsert behaviour. All sessions use *Commit Type = Target*, *Commit Interval = 10000* (except `s_m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins` = 50000), 1 partition, *Stop on errors = 0*, recovery disabled.

### 5.1 Monthly Account

| Session | Mapping | Treat rows as | Target writer settings (Observed) | Session-level SQL |
|---|---|---|---|---|
| `s_m_Agg_Divisional_Avg_insupd` | `m_Agg_Divisional_Avg_insupd` | Data driven | `Agg_Divisional_Avg`: Insert + **Update else Insert**; `BATCH_DW_LOAD_DETAIL`: Insert | – |
| `s_m_Fact_Monthly_Acct_Temp1_reload` | `m_Fact_Monthly_Acct_Temp1_reload` | Data driven | `ETL_FACT_MONTHLY_ACCT_TEMP1`: Insert, **Truncate** | – |
| `s_m_Fact_Monthly_Acct_Temp_reload` | `m_Fact_Monthly_Acct_Temp_reload` | Data driven | `ETL_FACT_MONTHLY_ACCT_TEMP`: Insert, **Truncate** | – |
| `s_m_Fact_Monthly_Acct_ins` | `m_Fact_Monthly_Acct_ins` | Data driven | `Fact_Monthly_Acct`: Insert; `BATCH_DW_LOAD_DETAIL`: Insert | – |
| `s_m_Fact_Monthly_Acct_upd` | `m_Fact_monthly_Acct_upd` | Data driven | Target instance `Fact_Monthly_Acct_upd` **renamed at session level to `ETL_FACT_MONTHLY_ACCT_UPD`**; Insert | **Post SQL:** `MERGE DWCORE.dbo.Fact_Monthly_Acct … USING (SELECT DISTINCT Monthly_Account_PK, Assigned_GL_Div_Nbr FROM ETL_FACT_MONTHLY_ACCT_UPD) … WHEN MATCHED THEN UPDATE SET Assigned_GL_Div_Nbr` |

Workflow-instance overrides re-point all readers to `$DBConnection_Source2` (`STG_IFP_CUAVTN` reader to `$DBConnection_Source3`) and audit lookups to `$DBConnection_Audit_RO`.

### 5.2 Sales Activity

| Session (reusable) | Instances in WF | Treat rows as | Target writer settings (Observed) | Session-level SQL / overrides |
|---|---|---|---|---|
| `s_m_Fact_Sales_Activity_Temp_Ifp_Summarybilled_ins` | 1 | Data driven | `ETL_FACT_SALES_ACTVY_TEMP`: Insert, **Truncate**; `STG_IFP_BIPSDCS`: **Update as Update** | – |
| `s_m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins` | 1 | Data driven | `Code_LOV`: Insert; `ETL_FACT_SALES_ACTVY_TEMP`: Insert; `Stg_Update` → **renamed `ETL_UPD_STG_IFP_BIPSD_TEMP`**: Insert | **Pre SQL** `exec usp_CreateDropTempTable 'STG_IFP_BIPSD','C'`; **Post SQL** `exec usp_UpdateStagingFromETL 'STG_IFP_BIPSD','DW'` |
| `s_m_Fact_Sales_Activity_Temp_Lawson_Stat_ins` | 1 | Insert | `ETL_FACT_SALES_ACTVY_TEMP`: Insert; `STG_LAW_DBGLGLT`: **Update as Update**; `BATCH_DW_LOAD_DETAIL`: Insert | – |
| `s_m_Fact_Sales_Activity_Temp_SP_upd` | 3 | Insert | `Dummy` (filter `FALSE` ⇒ no rows) | SP `usp_SetActivityReasonCd($$DB_SP_PARAM_VALUE)` |
| `s_m_Fact_Sales_Activity_SummeryBilled_Refresh_ins` | 1 | Insert | `Fact_Sales_Activity`: Insert; `BATCH_DW_LOAD_DETAIL`: Insert (Delete flag = YES, no effect with Insert rows) | – |
| `s_m_Fact_Sales_Activity_Refresh_ins` | 2 | Insert | `Fact_Sales_Activity`: Insert; `BATCH_DW_LOAD_DETAIL`: Insert | lookup connection overrides |
| `s_m_Fact_Sales_Activity_SummeryBilled_upd` | 1 | Insert | `Fact_Sales_Activity_Upd`: **Update as Update** (filter `1=2` ⇒ no rows reach it) | **SQL override** (join temp→fact, `Sales_Trans_Type='SDCS'`); **Post SQL** `UPDATE Fact_Sales_Activity SET is_Deleted, is_Updated, Upd_Batch_Id FROM … ETL_FACT_SALES_ACTVY_TEMP …` |
| `s_m_Fact_Sales_Activity_upd` | 2 | Insert | `Fact_Sales_Activity_Upd`: Update as Update (filter `FALSE` ⇒ no rows) | **SQL override** and **Post SQL** `UPDATE fact … INNER HASH JOIN ETL_FACT_SALES_ACTVY_TEMP … WHERE tmp.Sales_Trans_Type='$$SALES_TXN_TYP' …` |
| `s_m_Fact_Sales_Activity_Delta_ins` | 3 | Insert | `Fact_Sales_Activity`: Insert; `BATCH_DW_LOAD_DETAIL`: Insert | lookup connection overrides |
| `s_m_Code_Lov_LSC_insupd` | 1 | Data driven | `Code_LOV`: Insert + **Update else Insert** | – |
| `s_m_Agg_Prescriptive_Response_reload` | 1 | Insert | `Agg_Prescriptive_Response`: Insert; `BATCH_DW_LOAD_DETAIL`: Insert | **Session Pre SQL** builds `##fct_sls_acty` and **deletes** affected rows from `Agg_Prescriptive_Response`; **session SQL override** then selects source rows |
| `s_m_Agg_Sales_Activity_reload` | 1 | Data driven | `Agg_Sales_Activity`: Insert; `BATCH_DW_LOAD_DETAIL`: Insert | mapping-level **Pre SQL** builds `##fsa` from rows since `MAX(Ins_Batch_ID)` of `Agg_Sales_Activity` |
| `s_m_Dim_Sales_Comp_Pct_insupd` | 1 | Data driven | `Dim_Sales_Comp_Pct`: Insert (INS instance) / Update as Update (UPD instance) | flat-file reader `$PMSourceFileDir\$InputFileName` |
| `s_m_Rpt_Sales_Comp_Site_Reload` | 1 | Insert | `Rpt_Sales_Comp_Site`: Insert, **Truncate** | – |
| `s_m_Fact_Sales_Activity_reason_sk_fix_upd` | 1 | Data driven | `Fact_Sales_Activity`: Insert (INS instance) and Update as Update (UPD instance) | post-session success command (see §4.2) |
| `s_m_Collect_Table_Stats_SP_ins` | 1 | Insert | `Dummy` (no rows) | SP `usp_Collect_Table_Stats('Fact_Sales_Activity','Txn_Keyed_Dt_SK', <batch id>)` |
| `s_m_Rpt_Monthly_Employee_SP_ins` | 1 (disabled) | Insert | `Dummy` (no rows) | SP `usp_Load_Rpt_Monthly_Employee(201101, <YYYYMM of SYSDATE>)` |

Workflow-instance overrides route `ETL_FACT_SALES_ACTVY_TEMP` reads/writes to `$DBConnection_TempDB`, staging updates to `$DBConnection_Target2`, dimension lookups to `$DBConnection_Source2` (two lookups to `$DBConnection_Source`), and audit lookups to `$DBConnection_Audit_RO`.

---

## 6. Mapping inventory and Databricks implementation approach

Common building blocks appear in almost every mapping. They are described once here and referenced below.

**Common audit pattern ("AUDIT")** — Observed across most mappings; the `BATCH_DW_LOAD_DETAIL` audit insert is present in 12 of 22 mappings:
* Unconnected reusable lookup `lkp_Batch_id` (SQL override `SELECT max(BATCH_CNTRL_ID), 1 AS Dummy_Key FROM BATCH_AUDIT`) called as `:LKP.SC_LKP_BATCH_ID(1)` / `:LKP.LKP_BATCH_ID(1)` → current batch id stamped on rows as `Ins_Batch_ID` / `Upd_Batch_ID` / `ETL_BATCH_ID`.
* Unconnected reusable lookup `lkp_DSRC_ID` on `DATA_SOURCE` keyed by `$$DSRC_NM` → `DSRC_ID`.
* Aggregator `agg_Ins_Cnt` / `agg_Upd_Cnt` grouping by batch id + `DSRC_ID` → row count, `DATA_OPERATION='INS'|'UPD'`.
* Reusable expression `exp_Audit` adds `$$SRC_OBJ_NM`, `$$TGT_OBJ_NM`, `SESSSTARTTIME`, `$$INS_UPDT_UID`, `$$LOAD_ADDITI_TXT`, `$PMMappingName` → insert into `BATCH_DW_LOAD_DETAIL` (key `BATCH_LOAD_DETAIL_SK` is not mapped ⇒ Inference: DB-generated identity).

*Databricks:* one shared Python module/notebook: read current batch id once per job run (from a Delta `batch_audit` table or job parameter), broadcast-join or `lit()` it; compute counts with `df.groupBy(...).count()` or from Delta `MERGE` operation metrics (`numTargetRowsInserted/Updated`); append to a Delta `batch_dw_load_detail` table. Identity keys → Delta `GENERATED ALWAYS AS IDENTITY`.

**Surrogate-key defaulting pattern ("SKDEF")** — mapplet `mplt_dim_key_sk` (Observed): 32 reusable lookups on dimensions (`Dim_Acct`, `Dim_Site`, `Dim_Container_Grp`, `Dim_Corp_Hier`, `Dim_Infopro_Hier`, `Dim_Customer`, `Dim_Contract`, `Dim_Route`, `Dim_Charge_Cd`, `Dim_Employee*`, `Code_LOV`, …) followed by `expr_SK`, which converts lookup misses to `-1` and "not applicable" inputs to `-2`. Each of these lookups has its `Cache File Name Prefix` set to a parameter (`$$lkp_Dim_Acct`, `$$lkp_Dim_Site`, … 31 parameters, values only in the parameter file); this affects Informatica cache-file naming only and has no Databricks equivalent. 20 of the shared lookups also carry SQL overrides (e.g. `… FROM Dim_Territory WITH (NOLOCK)`) that must be ported.

*Databricks:* left joins (broadcast for small dimensions) to Delta dimension tables, then `F.when(...).otherwise(...)` / `coalesce` rules for -1/-2. Range lookups (`Eff_Dt <= x AND Disc_Dt >= x`) become non-equi joins; "Use Any Value" multiple-match policy must be made deterministic (e.g. `row_number()` over the lookup key) because Spark joins would otherwise duplicate rows.

**PSA write-back pattern ("PSA")** — mapplets `mplt_PSA_Update` (sorter distinct on `UNIQUE_ROW_ID` → `ROW_STATUS_CODE='P'`, `ETL_BATCH_ID` → `DD_UPDATE`) and `mplt_PSA_TEMP_Update` (`DD_INSERT` with `ROW_STATUS_CODE='P'`, `CURRENT_IND=''`, `PROCESS_BATCH='DW'`).
*Databricks:* Delta `MERGE INTO stg … WHEN MATCHED THEN UPDATE SET ROW_STATUS_CODE='P', ETL_BATCH_ID=…` keyed by `UNIQUE_ROW_ID`.

**SP-only mapping pattern ("SPWRAP")** — `Dummy` source with `SELECT 1`-style SQL (`len=15`), expression calls `:SP.<proc>(…)`, filter `FALSE`, `Dummy` target. The mapping does nothing except execute the stored procedure once.
*Databricks:* replace with a job task that runs the re-implemented procedure logic (Databricks SQL / PySpark). **Cannot be designed until the procedure bodies are supplied.**

**Post-SQL-update pattern ("POSTUPD")** — SQ override selects keys, filter `FALSE`/`1=2` blocks all rows, actual work is the target **Post SQL** `UPDATE … FROM … JOIN` / `MERGE`.
*Databricks:* a single Delta `MERGE INTO` (or `UPDATE` with subquery) statement; the PowerCenter pipeline itself is not needed.

### 6.1 `wf_Fact_Monthly_Acct_insupd` mappings

#### M1. `m_Agg_Divisional_Avg_insupd`
* **Sources (Observed):** `STG_IFP_CUAVTN` (DWPSA, source filter `current_ind='Y'`); `Fact_Monthly_Rate` ⋈ `Dim_Container_Grp` ⋈ `Dim_Rate_Hist` ⋈ `Dim_Charge_Cd` ⋈ `Dim_Corp_Hier` (user-defined join in SQ; filter `Charge_Cd IN ('REC','REG') AND Charge_Typ='S' AND Infopro_Div_Nbr<>'UNK' AND Report_Period_SK = <YYYYMM of GETDATE() minus Run_Loop_Contrl month offset>`).
* **Targets:** `Agg_Divisional_Avg` (PK `Monthly_Period_SK, Infopro_Div_Nbr`) via `upd_insupd` = `DD_UPDATE` with session *Update else Insert* ⇒ upsert; `BATCH_DW_LOAD_DETAIL` (AUDIT).
* **Transformations (56 transformation instances):** 25 Expression, 9 Sorter, 4 Aggregator, 4 Joiner (Normal ×3, Full Outer ×1, all sorted input), 4 Router, 3 Union (Custom Transformation `pmuniontrans`), 2 Filter, 2 unconnected lookups, 1 Update Strategy.
* **Logic (Observed):** hauls aggregated by period/division/`has_Compactor`/`Charge_Cd`, routed into 4 haul categories (open top / open top recycle / compactor / compactor recycle) by `Charge_Cd` and `has_Compactor`, unioned; tons from `STG_IFP_CUAVTN` routed into 4 categories by `AVTYPE`/`AVCMPR`, unioned; division-empty/not-empty split and re-union; full-outer join of hauls and tons; de-duplication via previous-row comparison in `exp_duplicate`, `exp_tons_val`, `exp_dup` (stateful variables after sorters) and filters `FLAG=1`.
* **Databricks:** `groupBy` + conditional aggregation (`sum(when(...))`) replaces router→union chains; full outer join on division; previous-row de-dup → `row_number()` over the same sort keys (must match sorter key order exactly). Write with Delta `MERGE` on (`Monthly_Period_SK`,`Infopro_Div_Nbr`). Period filter from a job parameter instead of `Run_Loop_Contrl` subquery, or read `run_loop_contrl` Delta table.

#### M2. `m_Fact_Monthly_Acct_Temp1_reload`
* **Sources (Observed):** four SQL-override SQs, all starting with `DECLARE @MONTH … FROM Run_Loop_Contrl`: `sq_Dim_Acct_Fee` (self-join of `DIM_ACCT_FEE` for fee types INV/LFC/FRF/SUS/EVR valid in the load month), `sq_Dim_Acct_Fee_FMA` (CTEs over `DIM_ACCT_FEE`, `Fact_Monthly_Acct`, `Dim_Acct`, `Code_LOV` — prior-month account status), `sq_Dim_Acct_Current` (`DIM_ACCT` with `ROW_NUMBER()` for de-dup, effective-dated to load month), `sq_Fact_Invoice_Detail` (`SUM(Invoice_Amt)` by division/account/charge-code bucket).
* **Target:** `ETL_FACT_MONTHLY_ACCT_TEMP1` (truncate + insert, 37 columns).
* **Transformations (25):** 15 lookups — 6 *unconnected* lookups on the **SQ pipeline** `sq_Fact_Invoice_Detail` (`lkp_FID_*_Fee_Rev`, "pipeline lookups") called from `exp_all_SK` via `:LKP.`; 5 connected pipeline lookups on `sq_Dim_Acct_Fee` and 1 on `sq_Dim_Acct_Fee_FMA`; reusable `lkp_Code_lov_FMA`, `lkp_Code_Lov_IDV_FMA`, `lkp_Batch_id`; 5 Expressions; 1 Filter (`Row_Number = 1`).
* **Logic (Observed):** per account, fee revenue by fee type, fee SK and "charged" flags per fee type (current vs prior), account status derived in `exp_chrg_SRF` from open/close YYYYMM vs load month (`NEW`, `LOST`, `NEW_LOST`, `ACTIVE`, `INACTIVE`) and compared with prior status (`Acct_Status_old`) to derive `Acct_Status` and `Acct_Code_Type`; `'F'/'0'` → `'0'` else `'1'` flag normalisation.
* **Databricks:** each SQ → Spark SQL view over Delta tables (T-SQL → Spark SQL rewrite: `DATEADD/DATEDIFF/CONVERT` → `add_months`, `date_trunc`, `date_format`). Pipeline lookups → joins to those DataFrames (pivot invoice amounts by charge code first, then one join instead of six). Nested `IIF` status logic → `F.when` chains. Write with `overwrite` (truncate semantics) to Delta.
* **Note:** expression `Acct_Code_Type` contains a commented-out line followed by live logic in the same expression text — the live part must be isolated carefully during conversion.

#### M3. `m_Fact_Monthly_Acct_Temp_reload`
* **Sources (Observed):** `ETL_FACT_MONTHLY_ACCT_TEMP1` (output of M2), `Dim_Acct` ⋈ `Dim_Contract_Fee`, `Fact_Monthly_Rate` ⋈ `Dim_Rate_Hist` ⋈ `Dim_Charge_Cd` ⋈ `Dim_Acct`, and five more `Dim_Rate_Hist` SQL-override SQs (`sq_Dim_Rate_Hist`, `_Resi`, `_industrial`, `_comm`, `_disposal`) each with `@pMONTH_NUM` from `Run_Loop_Contrl`, CTEs and `ROW_NUMBER()`.
* **Target:** `ETL_FACT_MONTHLY_ACCT_TEMP` (truncate + insert, 55 columns).
* **Transformations (52 transformation instances, plus 15 source instances):** 17 Expression, 8 SQ, 6 Joiner (5 Master Outer sorted, 1 Detail Outer), 6 lookups (4 pipeline lookups on SQ outputs — 2 connected, 2 unconnected `lkp_Dim_Rate_Hist_Resi`/`lkp_Dim_Rate_Hist_comm` — plus reusable `lkp_Dim_Division_Fee` on `Dim_Division_Fee` and `lkp_Batch_id`), 5 Sorter, 4 Aggregator (sorted), 4 Filter, 2 Router.
* **Logic (Observed):** "lost account" revenue by LOB (`rtr_lost_all` groups COMM_LOB / ALL_IND_LOB / IND_WITH_DISP / RESI_JOB using `ChargeCdChk`, `LOB_Category`, `is_Disposal_Billable`), revenue amount formula in variable ports (`Rate_Amt * Recurring_Charge_Freq / 12` for `Charge_Typ='R' AND Charge_Method='F'` etc.), per-account sums, master-outer joins back to the account set, division fee rates from `Dim_Division_Fee`.
* **Databricks:** conditional aggregation by `(Infopro_Div_Nbr, Acct_Nbr)` replaces router→sorter→aggregator→joiner fan-out; Master Outer Join in PowerCenter keeps all *detail* rows → Spark `left` join from the detail side (verify master/detail roles per joiner during build). Overwrite Delta table.

#### M4. `m_Fact_Monthly_Acct_ins`
* **Source:** `ETL_FACT_MONTHLY_ACCT_TEMP` (default SELECT).
* **Targets:** `Fact_Monthly_Acct` (39/40 columns mapped; PK `Monthly_Account_PK` not mapped ⇒ Inference: identity), `BATCH_DW_LOAD_DETAIL`.
* **Transformations:** SQ, `exp_fact_monthly_ins` (batch id; `Assigned_GL_Div = 0`), `upd_ins` (`DD_INSERT`), AUDIT.
* **Databricks:** straight append (`df.write.mode("append")`) to Delta with identity PK. Idempotency is not built into the PowerCenter mapping (no delete of the target month) — Inference: re-runs would duplicate a month; confirm whether a `replaceWhere` on `Report_Period_SK` is acceptable.

#### M5. `m_Fact_monthly_Acct_upd` (POSTUPD)
* **Sources:** three SQL-override SQs — `sq_Default_GL_Div_Nbr` (rank-1 GL division per InfoPro division), `sq_Fact_Monthly_Acct` (current load month rows with prior-month `Assigned_GL_Div_Nbr`), `sq_GL_Div_nbr` (GL division with max invoiced amount per account/period, `RANK()`).
* **Target:** writes `Monthly_Account_PK` + `Assigned_GL_Div_Nbr` into `ETL_FACT_MONTHLY_ACCT_UPD` (session table-name override), then Post-SQL `MERGE` into `Fact_Monthly_Acct`.
* **Logic (Observed):** `o_Assigned_GL_Div_Nbr = coalesce(Assigned_GL_Div_Nbr, src_Prev_Assigned_GL_Div_Nbr, Default_Div_Nbr)` after a detail-outer join and a normal join on default division.
* **Databricks:** three Spark SQL views, joins, `coalesce`, then one Delta `MERGE INTO fact_monthly_acct USING … ON Monthly_Account_PK WHEN MATCHED THEN UPDATE SET Assigned_GL_Div_Nbr`. The intermediate `ETL_FACT_MONTHLY_ACCT_UPD` table is unnecessary.

### 6.2 `wf_Fact_Sales_Activity_insupd` mappings

#### S1. `m_Fact_Sales_Activity_Temp_Ifp_Summarybilled_ins`
* **Sources:** `STG_IFP_BIPSDCS` ⋈ `STG_IFP_BIPIC` (DWPSA; user-defined join; filter `CURRENT_IND='Y' AND ROW_STATUS_CODE='I' AND SDPERD>=201001 AND SDTRCD<>'' AND SDRECD<>'' AND SDPROC='P'`).
* **Targets:** `ETL_FACT_SALES_ACTVY_TEMP` (truncate + insert, 101 cols); `STG_IFP_BIPSDCS` (update via `mplt_PSA_Update`, 3 cols).
* **Transformations:** 3 mapplet instances (2× SKDEF, 1× PSA), 10 lookups (incl. reusable `lkp_Dim_infopro_employee` ×2 with **"Use All Values"** multiple-match policy, `lkp_Dim_Acct_Fee` with a **4,249-char SQL override**, `lkp_Dim_Employee` ×2 with range conditions, `lkp_dim_contract`, `lkp_ETL_Mkt_Vertical_Xref`, `lkp_Dim_Contract_Grp`, `lkp_Infopro_Div`), 8 Sorters, 2 Joiners (sorted), 3 Filters, 10 Expressions (four with previous-row de-dup variables), 1 Update Strategy.
* **Databricks:** read staging Delta tables; dimension joins (SKDEF); "Use All Values" lookups → explicit join that returns multiple rows, followed by the existing sort + previous-row de-dup → `row_number()` over (`UNIQUE_ROW_ID` ASC, `Eff_Dt` DESC / `Key_Value` DESC) and keep row 1; lookup SQL override → Spark SQL view. Overwrite temp Delta table; `MERGE` row-status back into staging.

#### S2. `m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins`
* **Sources:** `STG_IFP_BIPSD` (same filter pattern as S1), `Dim_Rate_Hist` ⋈ `Dim_Charge_Cd` (filter `Charge_Cd='DSP' AND Charge_Typ='S' AND ISNULL(Waste_Material_Type,'')=''`).
* **Targets:** `Code_LOV` (insert of new code values), `ETL_FACT_SALES_ACTVY_TEMP` (append), `ETL_UPD_STG_IFP_BIPSD_TEMP` (via `mplt_PSA_TEMP_Update`; created by pre-SQL SP and applied back to staging by post-SQL SP).
* **Transformations:** 4 mapplet instances (3× SKDEF, 1× PSA_TEMP), 7 lookups (two with SQL overrides incl. a 4,117-char `lkp_Dim_Acct_Fee`), 12 Sorters, 4 Joiners, 7 Filters (incl. date-dependent `fil_Ratehist` using `$$Dsp_S_Q_Disc_Dt`), 16 Expressions (six stateful de-dup), 2 Update Strategies.
* **Databricks:** as S1; `Code_LOV` insert → `MERGE … WHEN NOT MATCHED THEN INSERT` (key `CODE_SK` not mapped ⇒ identity). The pre/post-SQL SP pair → a direct Delta `MERGE` into `stg_ifp_bipsd` — but only once `usp_CreateDropTempTable` / `usp_UpdateStagingFromETL` logic is supplied.

#### S3. `m_Fact_Sales_Activity_Temp_Lawson_Stat_ins`
* **Source:** `STG_LAW_DBGLGLT` (filter `CURRENT_IND='Y' AND ROW_STATUS_CODE IN ('C','I') AND ACCOUNT IN (92650, …)`).
* **Targets:** `ETL_FACT_SALES_ACTVY_TEMP` (append), `STG_LAW_DBGLGLT` (PSA update), `BATCH_DW_LOAD_DETAIL`.
* **Transformations:** 3 mapplet instances (2× SKDEF, 1× PSA), 5 reusable "is_current" lookups (`Dim_InfoPro_Hier`, `Dim_Acct` with override `Source_System='LAW'`, `Dim_Txn_Reason_Cd`, `Dim_Corp_Hier`, `Dim_Acct_Rep`), **flat-file lookup** `lkp_flt_Reason_Tran_Codes_Accounts` (`$PMSourceFileDir\..\SrcFiles\Reason_Tran_Codes_Accounts.txt`, pipe-delimited, 1 header row), 4 Expressions (`exp_common_transform` derives 79 ports; date conversions with `GET_DATE_PART`, `ADD_TO_DATE`).
* **Databricks:** flat file → Delta reference table (or `spark.read.csv(sep='|', header=True)` from a Volume); lookups → joins; expressions → column expressions. **File not provided.**

#### S4. `m_Fact_Sales_Activity_Temp_SP_upd` (SPWRAP; run 3×)
`:SP.USP_SETACTIVITYREASONCD($$DB_SP_PARAM_VALUE)`. Logic and per-pass parameter values unknown.

#### S5/S6. `m_Fact_Sales_Activity_SummeryBilled_Refresh_ins`, `m_Fact_Sales_Activity_Refresh_ins` (run 1× and 2×)
* **Source:** `ETL_FACT_SALES_ACTVY_TEMP` left-joined to `Fact_Sales_Activity` (user-defined join), filter `Sales_Activity_PK IS NULL AND DM_USER='NOT SET' AND DM_OPERATION_TYPE='I' AND Source_System = ISNULL(…)` ⇒ rows not yet in the fact.
* **Targets:** `Fact_Sales_Activity` insert (PK `Sales_Activity_PK` not mapped ⇒ identity), AUDIT.
* **Transformations:** lookups `lkp_Activity_Reason_SK` (`Dim_Sales_Activity_Hier`; design-time connection `DWCORE_DEVR1`, overridden in session), `lkp_Code_lov` (Refresh only), expressions deriving keys (`$$SALES_TXN_TYP`), `DD_INSERT`.
* **Databricks:** anti-join (`left_anti`) temp vs fact, or `MERGE … WHEN NOT MATCHED THEN INSERT` — the latter gives idempotency.

#### S7/S8. `m_Fact_Sales_Activity_SummeryBilled_upd`, `m_Fact_Sales_Activity_upd` (POSTUPD; run 1× and 2×)
Session SQL override + Post SQL `UPDATE Fact_Sales_Activity SET is_Deleted, is_Updated, Upd_Batch_Id` from temp rows joined on `BIPSDCS_UNIQUE_ROW_ID` (S7, `Sales_Trans_Type='SDCS'`) or `BIPSD_UNIQUE_ROW_ID` (S8, `Sales_Trans_Type='$$SALES_TXN_TYP'`, `Sales_Trans_Type_SK != 1`); only the AUDIT branch produces rows.
*Databricks:* one Delta `MERGE … WHEN MATCHED AND fact.is_Updated=0 AND fact.is_Deleted=0 THEN UPDATE`, plus audit count from MERGE metrics. Note the `INNER HASH JOIN` hint is T-SQL specific and is dropped.

#### S9. `m_Fact_Sales_Activity_Delta_ins` (run 3×)
* **Source:** `ETL_FACT_SALES_ACTVY_TEMP` filtered `DM_USER <> 'NOT SET' AND DM_OPERATION_TYPE IN ('I','A','U') AND Sales_Trans_Type='$$SALES_TXN_TYP'`.
* **Targets:** `Fact_Sales_Activity` insert, AUDIT. Lookups `lkp_code_lov` (`CODE_TYPE_CD='CTID'`), `lkp_Dim_Sales_Activity_Hier`.
* **Databricks:** filter + joins + append/`MERGE`. Behaviour per pass depends on `$$SALES_TXN_TYP` (parameter file).

#### S10. `m_Code_Lov_LSC_insupd`
`STG_IFP_CUPTB` (filter `DM_OPERATION_TYPE<>'D' AND CURRENT_IND='Y' AND TBTBLN='LSC'`) → `exp_lsc` → `DD_UPDATE` with session *Update else Insert* on `Code_LOV`.
*Databricks:* Delta `MERGE` on (`CODE_TYPE_CD`,`CODE_VALUE`) with identity `CODE_SK`. Note: `Code_LOV` target PK in the export is the composite (`CODE_SK`,`CODE_TYPE_CD`,`CODE_VALUE`) while `CODE_SK` is unmapped — the effective match key used by PowerCenter for "update" is the defined key columns that are connected; confirm the business key.

#### S11. `m_Agg_Prescriptive_Response_reload`
* **Source:** single SQ over 8 tables (`Fact_Sales_Activity`, `Dim_Acct`, `Dim_Corp_Hier`, `Code_LOV`, `Dim_Txn_Reason_Cd`, `Dim_Competitor`, `Dim_Site`, `Dim_Sales_Activity_Hier`) with a session-level Pre SQL that materialises `##fct_sls_acty` (rows with `Upd_Batch_Id >= MAX(Ins_Batch_ID)` of the aggregate) and **deletes** matching aggregate rows, and a session-level SQL override that selects source rows for re-aggregation.
* **Targets:** `Agg_Prescriptive_Response` (insert, 8-column composite key), AUDIT.
* **Transformations:** Router by `CODE_VALUE='SDCS'` vs other, two different aggregators (`agg_Measures_SDCS`, sorted `agg_Measures_SD`), Union, final aggregator `agg_Measures_SD_SDCS`.
* **Databricks:** incremental re-aggregation — compute affected key set from the fact's change feed (Delta CDF) or batch id; `MERGE` with `WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT`, or `DELETE` + `INSERT` in a single transaction via `replaceWhere`/`MERGE … WHEN NOT MATCHED BY SOURCE`. Global temp table becomes a temp view.

#### S12. `m_Agg_Sales_Activity_reload`
Same incremental pattern as S11 using pre-SQL `##fsa` (from `MAX(Ins_Batch_ID)` of `Agg_Sales_Activity`, default 190001). SQ over 8 tables incl. `Code_STR_Ragged_Total_Map`; `exp_Change_Key1` (16 variables) derives ragged-total keys; sorted aggregator `agg_Keys_Field`. Observed: the pre-SQL (1,103 chars) also deletes the affected rows from `Agg_Sales_Activity` (joined to `Dim_Employee` and `##fsa`) before the insert.

#### S13. `m_Dim_Sales_Comp_Pct_insupd`
* **Sources:** flat file `Sales_Comp_Pct` (comma-delimited, double-quote, 1 header row, MS1252, file name `$InputFileName`), `Dim_Sales_Comp_Pct` (SQL override).
* **Targets:** `Dim_Sales_Comp_Pct` INS / UPD instances, AUDIT (INS + UPD).
* **Logic:** detail-outer join on (`Infopro_Div_Nbr`,`Bus_Eff_Dt`), `exp_insupdflag` sets `Cdc_Flag` I/U, router splits, `$$env_HIGH_DATE` for open-ended dates.
* **Databricks:** Auto Loader / `spark.read.csv` from a Unity Catalog Volume → Delta `MERGE` (SCD-1 per observed I/U logic). **File and `$InputFileName` value not provided.**

#### S14. `m_Rpt_Sales_Comp_Site_Reload`
* **Sources:** 7 SQL-override SQs over `Rpt_Capture_Quote_Dtl` (×2, one 17,708-char query), `Rpt_Capture_Quote_Hdr`, `Fact_Sales_Activity` (×4, 2.8–7.8 KB each). SQL references additional undefined tables (`STG_CAP_QUOTE_HDR`, `STG_DIM_KEYS`, `STG_IFP_CUPAASAC`, `STG_IFP_CUPAASCN`, `Dim_Employee_Role`, …).
* **Target:** `Rpt_Sales_Comp_Site` (truncate + insert, 70/71 cols).
* **Transformations:** 6 Joiners (5 detail outer, 1 normal; unsorted), 12 Expressions (`exp_derived_columns`: 99 ports, 38 variables, `IS_NUMBER`), aggregator, router + union (null/not-null lift-change count), stateful de-dup (`exp_dup_flag`), reusable lookup `lkp_Dim_Sales_Comp_Pct`.
* **Databricks:** full rebuild → `overwrite` Delta table; port the SQL to Spark SQL. Highest SQL-conversion effort in the estate.

#### S15. `m_Fact_Sales_Activity_reason_sk_fix_upd`
SQL override (4,418 chars) over `Dim_Sales_Activity_Hier` and `Fact_Sales_Activity`; two update strategies (`DD_UPDATE`, `DD_INSERT`) feeding UPD / INS instances of `Fact_Sales_Activity`; AUDIT.
*Databricks:* single `MERGE` with matched-update / not-matched-insert clauses — but confirm the INS branch semantics (inserting into the fact from a "fix" job) with the data owner.

#### S16. `m_Collect_Table_Stats_SP_ins` (SPWRAP)
`usp_Collect_Table_Stats('Fact_Sales_Activity','Txn_Keyed_Dt_SK', <batch id>)`. Inference from the name: statistics maintenance. On Delta the closest equivalents are `ANALYZE TABLE … COMPUTE STATISTICS` and `OPTIMIZE … ZORDER BY (Txn_Keyed_Dt_SK)` / liquid clustering — confirm with SP body.

#### S17. `m_Rpt_Monthly_Employee_SP_ins` (SPWRAP; disabled)
`usp_Load_Rpt_Monthly_Employee(201101, YYYYMM(SYSDATE))`. Disabled in the workflow.

---

## 7. Data dependencies (table-level lineage, Observed)

| Table | Written by | Read by |
|---|---|---|
| `ETL_FACT_MONTHLY_ACCT_TEMP1` | M2 (truncate/insert) | M3 |
| `ETL_FACT_MONTHLY_ACCT_TEMP` | M3 (truncate/insert) | M4 |
| `ETL_FACT_MONTHLY_ACCT_UPD` | M5 (session table-name override) | M5 post-SQL |
| `Fact_Monthly_Acct` | M4 (insert), M5 (merge) | M2 (prior month status), M5 (prior GL div) |
| `Agg_Divisional_Avg` | M1 (upsert) | – |
| `ETL_FACT_SALES_ACTVY_TEMP` | S1 (truncate/insert), S2 (append), S3 (append) | S5–S9 (and SP `usp_SetActivityReasonCd` — inferred) |
| `Fact_Sales_Activity` | S5, S6, S9 (insert), S7, S8 (post-SQL update), S15 (insert/update) | S5/S6 (anti-join), S11, S12, S14, S15; `usp_Collect_Table_Stats` |
| `Code_LOV` | S2 (insert), S10 (upsert) | many lookups / SQs |
| `STG_IFP_BIPSDCS`, `STG_IFP_BIPSD`, `STG_LAW_DBGLGLT` | S1, S2 (via SPs), S3 — row-status update | S1, S2, S3 |
| `Dim_Sales_Comp_Pct` | S13 | S14 (lookup) |
| `Agg_Prescriptive_Response`, `Agg_Sales_Activity` | S11, S12 (delete+insert) | themselves (max batch id) |
| `Rpt_Sales_Comp_Site` | S14 (truncate/insert) | – |
| `BATCH_DW_LOAD_DETAIL` | AUDIT in 12 mappings | – |
| `BATCH_AUDIT`, `DATA_SOURCE`, `Run_Loop_Contrl` | **not written in these exports** | AUDIT lookups; Monthly SQs |

Cross-workflow coupling (Observed): both workflows read shared dimensions (`Dim_Acct`, `Dim_Charge_Cd`, `Dim_Rate_Hist`, `Dim_Corp_Hier`, `Dim_Container_Grp`, `Code_LOV`, …). `Code_LOV` is written by the Sales workflow and read by the Monthly workflow (lookups `lkp_Code_lov_FMA`, `lkp_Code_Lov_IDV_FMA` and the M2 SQ). No explicit ordering between the two workflows exists in the exports.

---

## 8. Missing metadata (must be supplied before build)

| # | Missing item | Why it matters | Where referenced |
|---|---|---|---|
| 1 | `wf_Fact_Monthly_Acct_insupd.par` and `wf_Fact_Sales_Activity.par` | Supplies all `$$` values and `$DBConnection_*` assignments; distinguishes the 3 passes of reused sessions | every session `Parameter Filename` |
| 2 | Values of 44 Sales and 6 Monthly `$$` parameters (e.g. `$$SALES_TXN_TYP`, `$$DB_SP_PARAM_VALUE`, `$$DSRC_NM`, `$$Dsp_S_Q_Disc_Dt`, `$$IFP_DSP_CALC_STRT_DT`, `$$pMONTH_NUM`, `$$env_HIGH_DATE`, `$$pBATCH_CNTRL_ID`). The 31 `$$lkp_*` parameters only name lookup cache files | Several are used in source filters, expressions and SP arguments; mapping defaults are mostly empty | mapping parameters, session SQL |
| 3 | Connection definitions for `$Source`, `$Target`, `$DBConnection_Source/Source2/Source3/Target/Target2/TempDB/Audit/Audit_RO`, `DWCORE_DEVR1` | Determines which physical database/schema each table is read from/written to (DWCORE vs DWPSA vs TempDB vs audit DB) | sessions, workflow-instance overrides |
| 4 | Stored procedure source: `usp_SetActivityReasonCd`, `usp_Collect_Table_Stats`, `usp_Load_Rpt_Monthly_Employee`, `usp_CreateDropTempTable`, `usp_UpdateStagingFromETL` | Business logic lives in the DB, not in PowerCenter | S2, S4, S16, S17 |
| 5 | DDL (columns, types, keys, identity, defaults, indexes) for tables referenced only in SQL/lookups: `Run_Loop_Contrl`, `BATCH_AUDIT`, `DATA_SOURCE`, `Dim_Division_Fee`, `Dim_Infopro_Employee`, `Dim_Employee`, `Dim_Employee_Role`, `Dim_Contract`, `Dim_Contract_Grp`, `Dim_Infopro_Div`, `Dim_Territory`, `Dim_Route`, `Dim_Service_Cd`, `Dim_Billing_Profile`, `Dim_Customer`, `Dim_Monthly_Period`, `Dim_Residential_Charge_Cd`, `Dim_Acct_Rep`, `ETL_Mkt_Vertical_Xref`, `ETL_FACT_MONTHLY_ACCT_UPD`, `ETL_UPD_STG_IFP_BIPSD_TEMP`, `STG_DIM_KEYS`, `STG_IFP_CUPAASAC`, `STG_IFP_CUPAASCN`, `STG_CAP_QUOTE_HDR` | Delta schemas cannot be created from the export alone | SQ/lookup overrides |
| 6 | Confirmation of identity/default columns (`Monthly_Account_PK`, `Sales_Activity_PK`, `Sales_Comp_Pct_SK`, `CODE_SK`, `BATCH_LOAD_DETAIL_SK`) | These PKs are never mapped — assumed DB-generated | target definitions |
| 7 | `Reason_Tran_Codes_Accounts.txt` (content + owner/refresh process) | Flat-file lookup in S3 | `lkp_flt_Reason_Tran_Codes_Accounts` |
| 8 | `Sales_Comp_Pct` input file sample and `$InputFileName` value | Source for S13 | session reader |
| 9 | Workflow `wf_Fact_Sales_Activity_IND_Trueup_ins` export and `start_workflow.sh` | Triggered on success of S15 | post-session command |
| 10 | Upstream jobs that load `DWPSA` staging (`STG_IFP_BIPSDCS`, `STG_IFP_BIPIC`, `STG_IFP_BIPSD`, `STG_LAW_DBGLGLT`, `STG_IFP_CUAVTN`, `STG_IFP_CUPTB`) and dimension loads | Required for end-to-end dependency ordering | sources |
| 11 | Process that sets `Run_Loop_Contrl.is_Processed` / `Loop_Nbr` and that inserts `BATCH_AUDIT` rows | Drives load period and batch id | Monthly SQs, AUDIT |
| 12 | External scheduler definition (time, calendar, predecessor jobs, how the two workflows are ordered) | Both workflows are `ONDEMAND` | `SCHEDULER` |
| 13 | `$PMFailureEmailUser` value and alerting expectations | Notification target | `email_FailureEmail` |
| 14 | Business rules / expected outputs (record counts, reconciliation reports) for parity testing | Needed to validate the migration | – |

---

## 9. Unsupported / high-risk constructs

No PowerCenter transformation type in these exports lacks a PySpark equivalent. The table classifies each construct by migration difficulty.

| Construct | Count | Where | Classification | Databricks treatment |
|---|---|---|---|---|
| Stored Procedure transformation (unconnected, `:SP.` call) | 3 SP transformations, 5 distinct procs incl. pre/post SQL | S2, S4, S16, S17 | **Blocked** — logic not in export | Re-implement as Spark SQL / PySpark after obtaining source |
| Pre/Post SQL with DML (`MERGE`, `UPDATE … FROM`, `DELETE`, `exec`) | 5 sessions + 1 mapping | M5, S2, S7, S8, S11, S12 | Manual rewrite | Delta `MERGE` / `UPDATE` / `DELETE` |
| SQL overrides with T-SQL (`DECLARE`, variables, CTEs, `##temp`, `WITH(NOLOCK)`, `INNER HASH JOIN`, `CONVERT`, `DATEADD`, `RANK`/`ROW_NUMBER`) | 31 mapping-level SQ SQL overrides, 3 session-level SQ overrides, 38 lookup SQL overrides (largest 17.7 KB) | M2, M3, M5, S1, S2, S11, S12, S14, S15 | Manual rewrite (largest effort) | Spark SQL views; parameters via job params; temp tables → temp views |
| Global temp tables `##fsa`, `##fct_sls_acty` | 2 | S11, S12 | Manual | temp views / CTEs |
| Session target-table-name override | 2 | M5, S2 | Easy but easy to miss | write to the real table name |
| Previous-row stateful expression variables (sorter + `prev_flag`) | ~14 expressions | M1, S1, S2, S14 | Semantics-sensitive | `row_number()`/`lag()` over identical partition + order; must be deterministic |
| Lookup policy "Use Any Value" | most lookups | all | Semantics-sensitive | deterministic de-dup of lookup side before join |
| Lookup policy "Use All Values" | 1 reusable (4 instances) | S1, S2 | Semantics-sensitive | plain join (multiplies rows) + downstream de-dup |
| Pipeline lookups (lookup source = SQ in the same mapping) | 16 | M2, M3 | Moderate | join to DataFrame |
| Unconnected lookups (`:LKP.`) | 11 distinct | M2, M3, AUDIT | Easy | join or broadcast scalar |
| Range (non-equi) lookups (`Eff_Dt <= x AND Disc_Dt >= x`) | several | S1, S2, `mplt_dim_key_sk` | Moderate | range join (consider bin-packing / `range_join` hint) |
| Flat-file lookup | 1 | S3 | Easy once file supplied | Delta reference table |
| Union (Custom Transformation `pmuniontrans`) | 5 | M1, S11, S14 | Easy | `unionByName` |
| Router | 9 | M1, M3, S11, S13, S14 | Easy | multiple filters or conditional aggregation |
| Joiner (Normal / Master Outer / Detail Outer / Full Outer; sorted input) | 25 | many | Easy, but master/detail mapping must be checked | Spark joins (sorted-input flag irrelevant) |
| Sorter (incl. distinct) | 42 | many | Mostly removable | only needed where it feeds stateful logic |
| Aggregator (incl. sorted input) | 25 | many | Easy | `groupBy().agg()` |
| Update Strategy `DD_INSERT/DD_UPDATE` + session "Update else Insert" | 15 (+2 in mapplets) | many | Easy | Delta `MERGE` |
| Truncate-and-load | 5 targets | M2, M3, S1, S14 | Easy | `mode("overwrite")` |
| SP-only "Dummy" mappings with `FALSE` filter | 3 mappings | S4, S16, S17 | Remove pattern | direct task |
| Workflow link condition on `SYSDATE` day | 1 | Monthly | Easy | job schedule / `if/else` condition task |
| Post-session success command (shell) launching another workflow | 1 | S15 | Moderate | Databricks Job `Run Job` task |
| Mapping params `$$…` / session vars `$PM…` / `SESSSTARTTIME` / `$PMMappingName` | 50+ | all | Easy once values known | job/task parameters, `current_timestamp()` captured once per run |
| Hard-coded design-time connection `DWCORE_DEVR1` on lookups | 5 | S1, S5, S6, S9 | Info | already overridden at session; use Unity Catalog names |
| Hard-coded literals (`SDPERD >= 201001`, `ACCOUNT IN (92650 …)`, `201101`) | several | S1, S2, S3, S17 | Info | keep as config, confirm still valid |
| Informatica functions used: `IIF`, `DECODE`-style nesting, `ISNULL`, `LTRIM/RTRIM`, `SUBSTR`, `INSTR`, `TO_CHAR`, `TO_DATE`, `ADD_TO_DATE`, `GET_DATE_PART`, `IS_NUMBER`, `SYSDATE`, `SESSSTARTTIME`, `ERROR` | – | expressions | Easy, with care for format masks and NULL semantics | `F.when`, `F.coalesce`, `F.trim`, `F.substring` (1-based both), `F.instr`, `F.date_format`, `F.to_date`, `F.add_months`/`date_add`, `F.expr("try_cast")` |

Specific semantic traps found:
* `TO_CHAR(Acct_Month)` comparisons with `YYYYMM` strings (M2) — Spark implicit casts differ; make types explicit.
* Informatica treats `''` and NULL differently from SQL Server; several expressions compare to `''` after `IIF(ISNULL(x),'',x)` — keep that order.
* `SUBSTR(TO_CHAR(SYSDATE,'YYYY-MM-DD'),…)` uses the Integration Service clock; in Databricks use a single run timestamp passed as a job parameter so all tasks agree.
* Default session `DateTime Format String` is `MM/DD/YYYY HH24:MI:SS.US`; implicit string↔date conversions inherit it.

---

## 10. Target Databricks architecture (proposal, no code)

* **Storage:** Unity Catalog catalog per environment; schemas mirroring `DWPSA` (bronze/staging), `DWCORE` (silver/gold dims & facts), `audit`. All tables Delta; identity columns for unmapped PKs; `TempDB` objects become job-scoped temp views or transient Delta tables in a `work` schema.
* **Ingestion:** staging tables either replicated from SQL Server (Lakeflow Connect / JDBC / CDC) or produced by migrated upstream loaders. Flat files land in UC Volumes and are read with Auto Loader.
* **Transformation:** one PySpark module per mapping, shared libraries for AUDIT, SKDEF (`mplt_dim_key_sk`), PSA write-back and lookup helpers (deterministic de-dup). T-SQL overrides converted to Spark SQL views. Writes via `MERGE`, `append`, or `overwrite` matching §5 session settings.
* **Orchestration:** one Databricks Job per workflow. Tasks mirror sessions; dependencies mirror links (`Status = SUCCEEDED` → "All succeeded"). Reused sessions become the same notebook with different task parameters (from the parameter file). Monthly day-5 gate → job schedule or an `If/else condition` task. S15 post-command → `Run Job` task for the true-up workflow. Failure email → job notifications.
* **Parameters/config:** parameter-file content migrated to job parameters / a config Delta table; connections replaced by UC table names; secrets in Databricks secret scopes.
* **Batch control:** replace `BATCH_AUDIT` max-id lookup and `Run_Loop_Contrl` with a Delta control table updated atomically at job start/end; Delta Change Data Feed can replace `Upd_Batch_Id`-based incremental detection in S11/S12.
* **Validation:** parallel run against SQL Server; row counts per `BATCH_DW_LOAD_DETAIL`, column-level checksums on facts/aggregates per period.

Suggested migration order (driven by dependencies): AUDIT + SKDEF libraries → Monthly M2 → M3 → M4 → M5 → M1 → Sales S10 → S1 → S2 → S3 → S5/S6 → S7/S8 → S9 → S13 → S15 → S11 → S12 → S14; SP-based steps (S4, S16, S2 pre/post) once procedure source is received; S17 only if re-enabled.

---

## 11. Open questions for the business / platform owners

1. Provide both parameter files (all sections) and the connection-to-database mapping.
2. Provide the five stored procedure definitions.
3. Is the Monthly workflow intended to run only on the 5th day of the month (as coded), and what triggers it?
4. How are the two workflows ordered relative to each other and to upstream staging/dimension loads?
5. Should `s_m_Rpt_Monthly_Employee_SP_ins` (disabled) be migrated?
6. Is `wf_Fact_Sales_Activity_IND_Trueup_ins` in scope? Please export it.
7. Should failures in `Temp_Ifp_SalesDetail` / `Refresh_ins` continue to be non-fatal to the workflow?
8. Confirm that M4 and the Sales "Delta"/"Refresh" inserts are expected to be non-idempotent today, and whether the Databricks version should be idempotent (recommended).
9. Who owns `Run_Loop_Contrl`, `BATCH_AUDIT`, `DATA_SOURCE`, and how are they maintained?
10. Are hard-coded literals (`SDPERD >= 201001`, Lawson account list, `201101`) still valid?

---

## Appendix A — Full object inventory (generated from the XML)

### A.1 `wf_Fact_Monthly_Acct_insupd.XML`


**Folder `ETL_Shared_Folder`** (shared=SHARED)

| Object type | Name | Type / DB | Detail |
|---|---|---|---|
| Reusable transformation | lkp_Dim_Division_Fee | Lookup Procedure | Dim_Division_Fee |
| Reusable transformation | lkp_Batch_id | Lookup Procedure | BATCH_AUDIT |
| Reusable transformation | lkp_Code_Lov_IDV_FMA | Lookup Procedure | Code_LOV |
| Reusable transformation | lkp_Code_lov_FMA | Lookup Procedure | Code_LOV |
| Reusable transformation | exp_Audit | Expression |  |
| Reusable transformation | lkp_DSRC_ID | Lookup Procedure | DATA_SOURCE |
| Source | Fact_Invoice_Detail | Microsoft SQL Server / DWCORE | 82 cols |
| Source | Dim_Container_Grp | Microsoft SQL Server / DWCORE | 99 cols |
| Source | Dim_Acct | Microsoft SQL Server / DWCORE | 54 cols |
| Source | Dim_InfoPro_Hier | Microsoft SQL Server / DWCORE | 21 cols |
| Source | Dim_GL_Hier | Microsoft SQL Server / DWCORE | 32 cols |
| Source | Fact_Monthly_Acct | Microsoft SQL Server / DWCORE | 40 cols |
| Source | Dim_Corp_Hier | Microsoft SQL Server / DWCORE | 60 cols |
| Source | Dim_Charge_Cd | Microsoft SQL Server / DWCORE | 10 cols |
| Source | Dim_Rate_Hist | Microsoft SQL Server / DWCORE | 44 cols |
| Source | ETL_FACT_MONTHLY_ACCT_TEMP1 | Microsoft SQL Server / DWCORE | 37 cols |
| Source | Dim_Contract_Fee | Microsoft SQL Server / DWCORE | 19 cols |
| Source | Fact_Monthly_Rate | Microsoft SQL Server / DWCORE | 26 cols |
| Source | Dim_Acct_Fee | Microsoft SQL Server / DWCORE | 22 cols |
| Source | ETL_FACT_MONTHLY_ACCT_TEMP | Microsoft SQL Server / DWCORE | 55 cols |
| Source | STG_IFP_CUAVTN | Microsoft SQL Server / DWPSA | 31 cols |
| Target | Fact_Monthly_Acct | Microsoft SQL Server | 40 cols; keys: Monthly_Account_PK |
| Target | ETL_FACT_MONTHLY_ACCT_TEMP | Microsoft SQL Server | 55 cols; keys: none |
| Target | ETL_FACT_MONTHLY_ACCT_TEMP1 | Microsoft SQL Server | 37 cols; keys: none |
| Target | BATCH_DW_LOAD_DETAIL | Microsoft SQL Server | 11 cols; keys: BATCH_LOAD_DETAIL_SK, BATCH_CNTRL_ID |
| Target | Agg_Divisional_Avg | Microsoft SQL Server | 11 cols; keys: Monthly_Period_SK, Infopro_Div_Nbr |

**Folder `ETL_Facts`** (shared=NOTSHARED)

| Object type | Name | Type / DB | Detail |
|---|---|---|---|
| Mapping | m_Fact_Monthly_Acct_ins | valid=YES | 10 instances, 219 port links |
| Shortcut | ETL_FACT_MONTHLY_ACCT_TEMP | Source Definition | -> ETL_Shared_Folder.ETL_FACT_MONTHLY_ACCT_TEMP |
| Shortcut | BATCH_DW_LOAD_DETAIL | Target Definition | -> ETL_Shared_Folder.BATCH_DW_LOAD_DETAIL |
| Shortcut | Fact_Monthly_Acct | Target Definition | -> ETL_Shared_Folder.Fact_Monthly_Acct |
| Shortcut | lkp_DSRC_ID | Lookup Procedure | -> ETL_Shared_Folder.lkp_DSRC_ID |
| Shortcut | lkp_Batch_id | Lookup Procedure | -> ETL_Shared_Folder.lkp_Batch_id |
| Shortcut | exp_Audit | Expression | -> ETL_Shared_Folder.exp_Audit |
| Session config | default_session_config |  |  |
| Task | email_FailureEmail | Email |  |
| Session | s_m_Fact_Monthly_Acct_ins | reusable=YES | mapping m_Fact_Monthly_Acct_ins |
| Mapping | m_Fact_Monthly_Acct_Temp1_reload | valid=YES | 30 instances, 197 port links |
| Shortcut | Dim_Acct | Source Definition | -> ETL_Shared_Folder.Dim_Acct |
| Shortcut | Dim_Acct_Fee | Source Definition | -> ETL_Shared_Folder.Dim_Acct_Fee |
| Shortcut | Fact_Invoice_Detail | Source Definition | -> ETL_Shared_Folder.Fact_Invoice_Detail |
| Shortcut | ETL_FACT_MONTHLY_ACCT_TEMP1 | Target Definition | -> ETL_Shared_Folder.ETL_FACT_MONTHLY_ACCT_TEMP1 |
| Shortcut | Shortcut_to_lkp_Code_lov_FMA | Lookup Procedure | -> ETL_Shared_Folder.lkp_Code_lov_FMA |
| Shortcut | Shortcut_to_lkp_Code_Lov_IDV_FMA | Lookup Procedure | -> ETL_Shared_Folder.lkp_Code_Lov_IDV_FMA |
| Session | s_m_Fact_Monthly_Acct_Temp1_reload | reusable=YES | mapping m_Fact_Monthly_Acct_Temp1_reload |
| Mapping | m_Agg_Divisional_Avg_insupd | valid=YES | 63 instances, 354 port links |
| Shortcut | STG_IFP_CUAVTN | Source Definition | -> ETL_Shared_Folder.STG_IFP_CUAVTN |
| Shortcut | Fact_Monthly_Rate | Source Definition | -> ETL_Shared_Folder.Fact_Monthly_Rate |
| Shortcut | Dim_Container_Grp | Source Definition | -> ETL_Shared_Folder.Dim_Container_Grp |
| Shortcut | Dim_Charge_Cd | Source Definition | -> ETL_Shared_Folder.Dim_Charge_Cd |
| Shortcut | Dim_Corp_Hier | Source Definition | -> ETL_Shared_Folder.Dim_Corp_Hier |
| Shortcut | Agg_Divisional_Avg | Target Definition | -> ETL_Shared_Folder.Agg_Divisional_Avg |
| Session | s_m_Agg_Divisional_Avg_insupd | reusable=YES | mapping m_Agg_Divisional_Avg_insupd |
| Mapping | m_Fact_Monthly_Acct_Temp_reload | valid=YES | 68 instances, 946 port links |
| Shortcut | Dim_Contract_Fee | Source Definition | -> ETL_Shared_Folder.Dim_Contract_Fee |
| Shortcut | Shortcut_to_ETL_FACT_MONTHLY_ACCT_TEMP1 | Source Definition | -> ETL_Shared_Folder.ETL_FACT_MONTHLY_ACCT_TEMP1 |
| Shortcut | Dim_Rate_Hist | Source Definition | -> ETL_Shared_Folder.Dim_Rate_Hist |
| Shortcut | ETL_FACT_MONTHLY_ACCT_TEMP | Target Definition | -> ETL_Shared_Folder.ETL_FACT_MONTHLY_ACCT_TEMP |
| Shortcut | Shortcut_to_lkp_Dim_Division_Fee | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_Division_Fee |
| Session | s_m_Fact_Monthly_Acct_Temp_reload | reusable=YES | mapping m_Fact_Monthly_Acct_Temp_reload |
| Mapping | m_Fact_monthly_Acct_upd | valid=YES | 22 instances, 71 port links |
| Shortcut | Fact_Monthly_Acct | Source Definition | -> ETL_Shared_Folder.Fact_Monthly_Acct |
| Shortcut | Dim_GL_Hier | Source Definition | -> ETL_Shared_Folder.Dim_GL_Hier |
| Shortcut | Shortcut_to_Dim_InfoPro_Hier | Source Definition | -> ETL_Shared_Folder.Dim_InfoPro_Hier |
| Session | s_m_Fact_Monthly_Acct_upd | reusable=YES | mapping m_Fact_monthly_Acct_upd |
| Workflow | wf_Fact_Monthly_Acct_insupd | valid=YES | 6 task instances, 5 links |

### A.2 `wf_Fact_Sales_Activity_insupd.XML`


**Folder `ETL_Shared_Folder`** (shared=SHARED)

| Object type | Name | Type / DB | Detail |
|---|---|---|---|
| Reusable transformation | lkp_Dim_Employee_FEPD | Lookup Procedure | Dim_Employee |
| Reusable transformation | lkp_corp_hier_fos | Lookup Procedure | Dim_Corp_Hier |
| Reusable transformation | lkp_Dim_Txn_Reason_Cd_SK_Source_Filter | Lookup Procedure | Dim_Txn_Reason_Cd |
| Reusable transformation | lkp_dim_route_FLD | Lookup Procedure | Dim_Route |
| Reusable transformation | lkp_Dim_Employee_RCQH | Lookup Procedure | Dim_Employee |
| Reusable transformation | lkp_Dim_Employee_FCR | Lookup Procedure | Dim_Employee |
| Reusable transformation | lkp_Dim_Route_eff | Lookup Procedure | Dim_Route |
| Reusable transformation | lkp_Dim_Service_Cd | Lookup Procedure | Dim_Service_Cd |
| Reusable transformation | lkp_Dim_Billing_Profile | Lookup Procedure | Dim_Billing_Profile |
| Reusable transformation | lkp_Dim_Corp_Hier_Ifp | Lookup Procedure | Dim_Corp_Hier |
| Reusable transformation | lkp_Dim_Acct_Fee | Lookup Procedure | Dim_Acct_Fee |
| Reusable transformation | lkp_Dim_Charge_cd | Lookup Procedure | Dim_Charge_Cd |
| Reusable transformation | lkp_Dim_Route | Lookup Procedure | Dim_Route |
| Reusable transformation | lkp_Dim_Ifp_Hier_ifp | Lookup Procedure | Dim_InfoPro_Hier |
| Reusable transformation | lkp_Dim_Residential_charge_cd | Lookup Procedure | Dim_Residential_Charge_Cd |
| Reusable transformation | lkp_Dim_Contract | Lookup Procedure | Dim_Contract |
| Reusable transformation | lkp_Dim_Territory | Lookup Procedure | Dim_Territory |
| Reusable transformation | lkp_Dim_Infopro_Hier | Lookup Procedure | Dim_InfoPro_Hier |
| Reusable transformation | lkp_Dim_Txn_Reason_Cd | Lookup Procedure | Dim_Txn_Reason_Cd |
| Reusable transformation | lkp_Dim_Site | Lookup Procedure | Dim_Site |
| Reusable transformation | lkp_Dim_Competitor | Lookup Procedure | Dim_Competitor |
| Reusable transformation | lkp_Dim_Infopro_Div | Lookup Procedure | Dim_Infopro_Div |
| Reusable transformation | lkp_Acct_Rep | Lookup Procedure | Dim_Acct_Rep |
| Reusable transformation | lkp_Dim_Acct | Lookup Procedure | Dim_Acct |
| Reusable transformation | lkp_Dim_Corp_Hier | Lookup Procedure | Dim_Corp_Hier |
| Reusable transformation | lkp_Code_lov | Lookup Procedure | Code_LOV |
| Reusable transformation | lkp_Dim_Contract_Grp | Lookup Procedure | Dim_Contract_Grp |
| Reusable transformation | lkp_Dim_Employee | Lookup Procedure | Dim_Employee |
| Reusable transformation | lkp_Dim_Customer | Lookup Procedure | Dim_Customer |
| Reusable transformation | lkp_Dim_Infopro_Hier_fil | Lookup Procedure | Dim_InfoPro_Hier |
| Reusable transformation | lkp_Dim_Monthly_Period | Lookup Procedure | Dim_Monthly_Period |
| Reusable transformation | lkp_Dim_Container_Grp | Lookup Procedure | Dim_Container_Grp |
| Mapplet | mplt_dim_key_sk | Mapplet | 2x Expression, 32x Lookup Procedure, 1x Input Transformation, 1x Output Transformation |
| Reusable transformation | lkp_Batch_id | Lookup Procedure | BATCH_AUDIT |
| Mapplet | mplt_PSA_Update | Mapplet | 1x Update Strategy, 1x Expression, 1x Input Transformation, 1x Output Transformation, 1x Sorter, 1x Lookup Procedure |
| Mapplet | mplt_PSA_TEMP_Update | Mapplet | 1x Update Strategy, 1x Expression, 1x Input Transformation, 1x Output Transformation, 1x Lookup Procedure |
| Reusable transformation | exp_Audit | Expression |  |
| Reusable transformation | lkp_DSRC_ID | Lookup Procedure | DATA_SOURCE |
| Reusable transformation | lkp_Dim_infopro_employee | Lookup Procedure | Dim_Infopro_Employee |
| Reusable transformation | lkp_Dim_Infopro_Hier_is_current | Lookup Procedure | Dim_InfoPro_Hier |
| Reusable transformation | lkp_Dim_Acct_Is_Current | Lookup Procedure | Dim_Acct |
| Reusable transformation | lkp_Dim_Txn_Reason_Cd_Is_Curr | Lookup Procedure | Dim_Txn_Reason_Cd |
| Reusable transformation | lkp_Dim_Corp_Hier_is_current | Lookup Procedure | Dim_Corp_Hier |
| Reusable transformation | lkp_Dim_Acct_Rep_Is_Current | Lookup Procedure | Dim_Acct_Rep |
| Reusable transformation | lkp_Dim_Sales_Comp_Pct | Lookup Procedure | Dim_Sales_Comp_Pct |
| Reusable transformation | usp_Load_Rpt_Monthly_Employee | Stored Procedure | usp_Load_Rpt_Monthly_Employee |
| Source | ETL_FACT_SALES_ACTVY_TEMP | Microsoft SQL Server / DWCORE | 101 cols |
| Source | Fact_Sales_Activity | Microsoft SQL Server / DWCORE | 69 cols |
| Source | Dim_Corp_Hier | Microsoft SQL Server / DWCORE | 60 cols |
| Source | Code_LOV | Microsoft SQL Server / DWCORE | 8 cols |
| Source | Dim_Txn_Reason_Cd | Microsoft SQL Server / DWCORE | 7 cols |
| Source | Dim_Competitor | Microsoft SQL Server / DWCORE | 14 cols |
| Source | Dim_Site | Microsoft SQL Server / DWCORE | 64 cols |
| Source | Dim_Sales_Activity_Hier | Microsoft SQL Server / DWCORE | 24 cols |
| Source | Dim_Acct | Microsoft SQL Server / DWCORE | 54 cols |
| Source | Dim_Sales_Comp_Pct | Microsoft SQL Server / DWCORE | 26 cols |
| Source | Sales_Comp_Pct | Flat File / FlatFile | 19 cols |
| Source | STG_IFP_BIPIC | Microsoft SQL Server / DWPSA | 25 cols |
| Source | STG_IFP_BIPSDCS | Microsoft SQL Server / DWPSA | 53 cols |
| Source | Dim_Charge_Cd | Microsoft SQL Server / DWCORE | 10 cols |
| Source | Dim_Rate_Hist | Microsoft SQL Server / DWCORE | 44 cols |
| Source | STG_IFP_BIPSD | Microsoft SQL Server / DWPSA | 93 cols |
| Source | Code_STR_Ragged_Total_Map | Microsoft SQL Server / DWCORE | 6 cols |
| Source | STG_LAW_DBGLGLT | Microsoft SQL Server / DWPSA | 73 cols |
| Source | Rpt_Capture_Quote_Dtl | Microsoft SQL Server / DWCORE | 154 cols |
| Source | Rpt_Capture_Quote_Hdr | Microsoft SQL Server / DWCORE | 115 cols |
| Source | STG_IFP_CUPTB | Microsoft SQL Server / DWPSA | 26 cols |
| Target | Fact_Sales_Activity | Microsoft SQL Server | 69 cols; keys: Sales_Activity_PK |
| Target | BATCH_DW_LOAD_DETAIL | Microsoft SQL Server | 11 cols; keys: BATCH_LOAD_DETAIL_SK, BATCH_CNTRL_ID |
| Target | Agg_Prescriptive_Response | Microsoft SQL Server | 17 cols; keys: Sales_Activity_Period_SK, Cur_Div_SK, Cur_LOB_Category_SK, Corp_Hier_SK, Activity_Reason_SK, Competitor_SK, Emp_Signing_Rep_SK, CODE_VALUE |
| Target | Dim_Sales_Comp_Pct | Microsoft SQL Server | 26 cols; keys: Sales_Comp_Pct_SK |
| Target | ETL_FACT_SALES_ACTVY_TEMP | Microsoft SQL Server | 101 cols; keys: none |
| Target | STG_IFP_BIPSDCS | Microsoft SQL Server | 53 cols; keys: UNIQUE_ROW_ID |
| Target | Stg_Update | Microsoft SQL Server | 7 cols; keys: none |
| Target | Code_LOV | Microsoft SQL Server | 8 cols; keys: CODE_SK, CODE_TYPE_CD, CODE_VALUE |
| Target | Agg_Sales_Activity | Microsoft SQL Server | 25 cols; keys: Sales_Activity_Period_SK, Cur_Div_SK, Cur_LOB_Category_SK, Activity_Reason_SK, Emp_Signing_Rep_SK |
| Target | STG_LAW_DBGLGLT | Microsoft SQL Server | 73 cols; keys: UNIQUE_ROW_ID |
| Target | Rpt_Sales_Comp_Site | Microsoft SQL Server | 71 cols; keys: none |

**Folder `ETL_Facts`** (shared=NOTSHARED)

| Object type | Name | Type / DB | Detail |
|---|---|---|---|
| Mapping | m_Dim_Sales_Comp_Pct_insupd | valid=YES | 22 instances, 317 port links |
| Shortcut | Sales_Comp_Pct | Source Definition | -> ETL_Shared_Folder.Sales_Comp_Pct |
| Shortcut | Dim_Sales_Comp_Pct | Source Definition | -> ETL_Shared_Folder.Dim_Sales_Comp_Pct |
| Shortcut | BATCH_DW_LOAD_DETAIL | Target Definition | -> ETL_Shared_Folder.BATCH_DW_LOAD_DETAIL |
| Shortcut | Dim_Sales_Comp_Pct | Target Definition | -> ETL_Shared_Folder.Dim_Sales_Comp_Pct |
| Shortcut | lkp_DSRC_ID | Lookup Procedure | -> ETL_Shared_Folder.lkp_DSRC_ID |
| Shortcut | lkp_Batch_id | Lookup Procedure | -> ETL_Shared_Folder.lkp_Batch_id |
| Shortcut | exp_Audit | Expression | -> ETL_Shared_Folder.exp_Audit |
| Session config | default_session_config |  |  |
| Session | s_m_Dim_Sales_Comp_Pct_insupd | reusable=YES | mapping m_Dim_Sales_Comp_Pct_insupd |
| Mapping | m_Agg_Prescriptive_Response_reload | valid=YES | 26 instances, 258 port links |
| Shortcut | Dim_Acct | Source Definition | -> ETL_Shared_Folder.Dim_Acct |
| Shortcut | Fact_Sales_Activity | Source Definition | -> ETL_Shared_Folder.Fact_Sales_Activity |
| Shortcut | Dim_Sales_Activity_Hier | Source Definition | -> ETL_Shared_Folder.Dim_Sales_Activity_Hier |
| Shortcut | Dim_Site | Source Definition | -> ETL_Shared_Folder.Dim_Site |
| Shortcut | Dim_Competitor | Source Definition | -> ETL_Shared_Folder.Dim_Competitor |
| Shortcut | Dim_Txn_Reason_Cd | Source Definition | -> ETL_Shared_Folder.Dim_Txn_Reason_Cd |
| Shortcut | Code_LOV | Source Definition | -> ETL_Shared_Folder.Code_LOV |
| Shortcut | Dim_Corp_Hier | Source Definition | -> ETL_Shared_Folder.Dim_Corp_Hier |
| Shortcut | Agg_Prescriptive_Response | Target Definition | -> ETL_Shared_Folder.Agg_Prescriptive_Response |
| Session | s_m_Agg_Prescriptive_Response_reload | reusable=YES | mapping m_Agg_Prescriptive_Response_reload |
| Mapping | m_Fact_Sales_Activity_Refresh_ins | valid=YES | 13 instances, 328 port links |
| Shortcut | ETL_FACT_SALES_ACTVY_TEMP1 | Source Definition | -> ETL_Shared_Folder.ETL_FACT_SALES_ACTVY_TEMP |
| Shortcut | Fact_Sales_Activity | Target Definition | -> ETL_Shared_Folder.Fact_Sales_Activity |
| Shortcut | lkp_Code_lov | Lookup Procedure | -> ETL_Shared_Folder.lkp_Code_lov |
| Session | s_m_Fact_Sales_Activity_Refresh_ins | reusable=YES | mapping m_Fact_Sales_Activity_Refresh_ins |
| Mapping | m_Fact_Sales_Activity_Temp_Lawson_Stat_ins | valid=YES | 22 instances, 464 port links |
| Shortcut | mplt_PSA_Update | Mapplet | -> ETL_Shared_Folder.mplt_PSA_Update |
| Shortcut | mplt_dim_key_sk | Mapplet | -> ETL_Shared_Folder.mplt_dim_key_sk |
| Shortcut | STG_LAW_DBGLGLT | Source Definition | -> ETL_Shared_Folder.STG_LAW_DBGLGLT |
| Shortcut | STG_LAW_DBGLGLT | Target Definition | -> ETL_Shared_Folder.STG_LAW_DBGLGLT |
| Shortcut | ETL_FACT_SALES_ACTVY_TEMP | Target Definition | -> ETL_Shared_Folder.ETL_FACT_SALES_ACTVY_TEMP |
| Shortcut | lkp_Dim_Acct_Rep_Is_Current | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_Acct_Rep_Is_Current |
| Shortcut | lkp_Dim_Corp_Hier_is_current | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_Corp_Hier_is_current |
| Shortcut | lkp_Dim_Txn_Reason_Cd_Is_Curr | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_Txn_Reason_Cd_Is_Curr |
| Shortcut | lkp_Dim_Acct_Is_Current | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_Acct_Is_Current |
| Shortcut | lkp_Dim_Infopro_Hier_is_current | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_Infopro_Hier_is_current |
| Session | s_m_Fact_Sales_Activity_Temp_Lawson_Stat_ins | reusable=YES | mapping m_Fact_Sales_Activity_Temp_Lawson_Stat_ins |
| Mapping | m_Fact_Sales_Activity_SummeryBilled_upd | valid=YES | 11 instances, 30 port links |
| Session | s_m_Fact_Sales_Activity_SummeryBilled_upd | reusable=YES | mapping m_Fact_Sales_Activity_SummeryBilled_upd |
| Mapping | m_Rpt_Sales_Comp_Site_Reload | valid=YES | 42 instances, 1170 port links |
| Shortcut | Rpt_Capture_Quote_Hdr | Source Definition | -> ETL_Shared_Folder.Rpt_Capture_Quote_Hdr |
| Shortcut | Rpt_Capture_Quote_Dtl | Source Definition | -> ETL_Shared_Folder.Rpt_Capture_Quote_Dtl |
| Shortcut | Rpt_Sales_Comp_Site | Target Definition | -> ETL_Shared_Folder.Rpt_Sales_Comp_Site |
| Shortcut | lkp_Dim_Sales_Comp_Pct | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_Sales_Comp_Pct |
| Session | s_m_Rpt_Sales_Comp_Site_Reload | reusable=YES | mapping m_Rpt_Sales_Comp_Site_Reload |
| Mapping | m_Fact_Sales_Activity_Delta_ins | valid=YES | 12 instances, 307 port links |
| Session | s_m_Fact_Sales_Activity_Delta_ins | reusable=YES | mapping m_Fact_Sales_Activity_Delta_ins |
| Source | Dummy | Microsoft SQL Server / Microsoft_SQL_Server | 1 cols |
| Target | Dummy | Microsoft SQL Server | 1 cols; keys: none |
| Reusable transformation | usp_SetActivityReasonCd | Stored Procedure | usp_SetActivityReasonCd |
| Mapping | m_Fact_Sales_Activity_Temp_SP_upd | valid=YES | 6 instances, 4 port links |
| Session | s_m_Fact_Sales_Activity_Temp_SP_upd | reusable=YES | mapping m_Fact_Sales_Activity_Temp_SP_upd |
| Mapping | m_Fact_Sales_Activity_reason_sk_fix_upd | valid=YES | 13 instances, 291 port links |
| Session | s_m_Fact_Sales_Activity_reason_sk_fix_upd | reusable=YES | mapping m_Fact_Sales_Activity_reason_sk_fix_upd |
| Mapping | m_Agg_Sales_Activity_reload | valid=YES | 20 instances, 201 port links |
| Shortcut | STR_Ragged_Total_Map | Source Definition | -> ETL_Shared_Folder.Code_STR_Ragged_Total_Map |
| Shortcut | Agg_Sales_Activity | Target Definition | -> ETL_Shared_Folder.Agg_Sales_Activity |
| Session | s_m_Agg_Sales_Activity_reload | reusable=YES | mapping m_Agg_Sales_Activity_reload |
| Mapping | m_Fact_Sales_Activity_SummeryBilled_Refresh_ins | valid=YES | 11 instances, 266 port links |
| Session | s_m_Fact_Sales_Activity_SummeryBilled_Refresh_ins | reusable=YES | mapping m_Fact_Sales_Activity_SummeryBilled_Refresh_ins |
| Mapping | m_Fact_Sales_Activity_Temp_Ifp_Summarybilled_ins | valid=YES | 42 instances, 1000 port links |
| Shortcut | STG_IFP_BIPSDCS | Source Definition | -> ETL_Shared_Folder.STG_IFP_BIPSDCS |
| Shortcut | STG_IFP_BIPIC | Source Definition | -> ETL_Shared_Folder.STG_IFP_BIPIC |
| Shortcut | STG_IFP_BIPSDCS | Target Definition | -> ETL_Shared_Folder.STG_IFP_BIPSDCS |
| Shortcut | lkp_Dim_infopro_employee | Lookup Procedure | -> ETL_Shared_Folder.lkp_Dim_infopro_employee |
| Session | s_m_Fact_Sales_Activity_Temp_Ifp_Summarybilled_ins | reusable=YES | mapping m_Fact_Sales_Activity_Temp_Ifp_Summarybilled_ins |
| Mapping | m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins | valid=YES | 60 instances, 1508 port links |
| Shortcut | mplt_PSA_TEMP_Update | Mapplet | -> ETL_Shared_Folder.mplt_PSA_TEMP_Update |
| Shortcut | STG_IFP_BIPSD | Source Definition | -> ETL_Shared_Folder.STG_IFP_BIPSD |
| Shortcut | Dim_Rate_Hist | Source Definition | -> ETL_Shared_Folder.Dim_Rate_Hist |
| Shortcut | Dim_Charge_Cd | Source Definition | -> ETL_Shared_Folder.Dim_Charge_Cd |
| Shortcut | Code_LOV | Target Definition | -> ETL_Shared_Folder.Code_LOV |
| Shortcut | Stg_Update | Target Definition | -> ETL_Shared_Folder.Stg_Update |
| Session | s_m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins | reusable=YES | mapping m_Fact_Sales_Activity_Temp_Ifp_SalesDetail_ins |
| Mapping | m_Rpt_Monthly_Employee_SP_ins | valid=YES | 6 instances, 4 port links |
| Shortcut | usp_Load_Rpt_Monthly_Employee | Stored Procedure | -> ETL_Shared_Folder.usp_Load_Rpt_Monthly_Employee |
| Session | s_m_Rpt_Monthly_Employee_SP_ins | reusable=YES | mapping m_Rpt_Monthly_Employee_SP_ins |
| Reusable transformation | usp_Collect_Table_Stats | Stored Procedure | usp_Collect_Table_Stats |
| Mapping | m_Collect_Table_Stats_SP_ins | valid=YES | 7 instances, 4 port links |
| Session | s_m_Collect_Table_Stats_SP_ins | reusable=YES | mapping m_Collect_Table_Stats_SP_ins |
| Mapping | m_Code_Lov_LSC_insupd | valid=YES | 6 instances, 18 port links |
| Shortcut | STG_IFP_CUPTB | Source Definition | -> ETL_Shared_Folder.STG_IFP_CUPTB |
| Task | email_FailureEmail | Email |  |
| Session | s_m_Code_Lov_LSC_insupd | reusable=YES | mapping m_Code_Lov_LSC_insupd |
| Mapping | m_Fact_Sales_Activity_upd | valid=YES | 11 instances, 29 port links |
| Session | s_m_Fact_Sales_Activity_upd | reusable=YES | mapping m_Fact_Sales_Activity_upd |
| Workflow | wf_Fact_Sales_Activity_insupd | valid=YES | 24 task instances, 23 links |
