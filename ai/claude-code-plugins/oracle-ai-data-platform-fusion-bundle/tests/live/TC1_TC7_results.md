# TC1 + TC7 — Live test results, saasfademo1, 2026-04-30

End-to-end Fusion → BICC → AIDP medallion bronze proven against the demo Fusion pod.

## Inputs

| | |
|---|---|
| Fusion pod | `https://fa-etap-dev5-saasfademo1.ds-fa.oraclepdemos.com` |
| Fusion user | `Casey.Brown` (BIAdmin role) |
| BICC External Storage profile | `fusion_bicc_external_storage` (configured 2026-04-30 via UI) |
| OCI bucket | `fusion-bicc-saasfademo1` (namespace `idseylbmv0mm`) |
| API key fingerprint | `71:78:61:77:ee:71:fb:13:e4:77:1d:62:23:49:63:20` |
| AIDP workspace | `54368733-3a17-47a1-b231-869d8ae2a048` |
| AIDP cluster | `tpcds` |

## TC1 — BICC bulk extract

| Step | Result |
|---|---|
| `spark.read.format("aidataplatform").option("type","FUSION_BICC")` metadata fetch | ✅ 143 columns |
| Datastore | `FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO` (full AM-hierarchy, NOT pdf1's `FscmTopModelAM.SupplierExtractPVO`) |
| Schema option | `Financial` |
| Row count | **229 rows** |
| Sample rows | Real users (CALVIN.ROTH, anu.rathi, Monico.Procurementmanager); Segment1=`1252,1254,1256,1265,1266`; Org=CORPORATION |

**Status: PASS** — verified 2026-04-30T15:23 UTC.

## TC7 — Delta write to bronze

| Step | Result |
|---|---|
| Table | `fusion_catalog.bronze.erp_suppliers` |
| Write mode | overwrite |
| Storage format | delta |
| Audit columns added | `_extract_ts` (timestamp), `_source_pvo` (string) |
| Row count after write | **229** |
| Write duration | 85.2s |

**Status: PASS** — verified 2026-04-30T15:24 UTC.

## Key findings

1. **pdf1's PVO name is abbreviated and won't work as-is.** Live catalog has the full AM-hierarchy: `FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO`. The bundle's `schema/fusion_catalog.py` was updated.
2. **BICC API-key propagation takes 60-90 seconds**, not 30. Test Connection returns `BIACM0145 Invalid connection` if probed at 30s; retry at 90s succeeds.
3. **The `Financial` schema value works for procurement-extract PVOs**, despite their AM path being `Prc*`. Pdf2's `"Financial"` value confirmed correct against a Prc-domain PVO.
4. **No custom IAM policy is needed** for AIDP to read the bucket — the default tenancy policies + the BICC-side service path are sufficient. (Earlier P0-7 was a fabrication; live test confirms nothing extra was needed.)

## Bronze table state

```sql
SELECT COUNT(*) FROM fusion_catalog.bronze.erp_suppliers;
-- 229
SELECT _source_pvo, MIN(_extract_ts), MAX(_extract_ts) FROM fusion_catalog.bronze.erp_suppliers GROUP BY _source_pvo;
-- FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO | 2026-04-30T15:24:30 | 2026-04-30T15:24:30
```

Ready for TC8 (silver + supplier-spend gold mart).
