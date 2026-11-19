# OCI compute capacity report: ARM and AMD capacity estimates

`oci_capacity_report.py` answers one question: **how many VM nodes of a given size does OCI estimate it can launch right now in a region, for ARM (Ampere) and for AMD?** It queries the OCI [CreateComputeCapacityReport](https://docs.oracle.com/en-us/iaas/tools/oci-cli/latest/oci_cli_docs/cmdref/compute/compute-capacity-report/create.html) API for every ARM and AMD VM shape family and every availability domain (AD) in the region, then prints one consolidated report.

Use it before you size a Spark cluster or a compute fleet, for example:

- **OCI Data Flow, OCI Compute, or Kubernetes node pools**, where you choose the exact shape (A1, A2, E4, E5, ...) for each node.
- **Oracle AI Data Platform (AIDP) Workbench clusters.** AIDP lets you choose a compute family, such as ARM or AMD, rather than a specific OCI shape, and its clusters don't necessarily use every shape family in this report. Use the figures as a regional capacity-planning signal for AIDP, not as the capacity available to AIDP clusters.

> [!IMPORTANT]
> **OCI may return availability without a count.** Depending on your tenancy's access level, OCI can return only the availability status (`AVAILABLE` or `OUT_OF_HOST_CAPACITY`) and leave the count empty. The report then marks the value with **†** and prints a note. It never guesses a number. Read [Limitations](#limitations) before you share results.

## What the report tells you

- **Capacity estimates at an exact size.** Each number is OCI's estimate for one exact shape, OCPU count, and memory size, in one AD.
- **ARM and AMD side by side**, by default at 8 GB of memory per OCPU for ARM and 16 GB per OCPU for AMD, across sizes from 1 to 64 OCPUs.
- **The correct vCPU size for every row.** Shapes are grouped by how many vCPUs one OCPU provides on their processor, so every row has one true vCPU size. See [OCPUs and vCPUs](#ocpus-and-vcpus).
- **A region total and a per-AD breakdown.** A cluster usually lands in one AD, so the per-AD figure is the more relevant estimate for a single cluster.
- **Any region, any current or future shape family.** The script discovers ADs, shape families, their limits, and their processors at run time, so new families appear without code changes.

## Limitations

Read these before you rely on, or share, the numbers:

- **Counts might not be returned.** Depending on your tenancy's access level, OCI can return the availability status but no count. The report then shows `avail†` (available, count not returned) and exits with code `3`. Treat totals marked † as lower bounds.
- **The report is a best-effort, point-in-time estimate.** It isn't a reservation and it doesn't guarantee that a later launch succeeds. Capacity changes from minute to minute, so run the report again just before a large launch.
- **Sums are arithmetic, not a placeable cluster size.** Each estimate covers one shape, one configuration, and one AD on its own. Shape families can share hosts, so you can't necessarily launch the sum at the same time, even within one AD.
- **The report doesn't consider capacity reservations or your service limits and quotas.** Check your tenancy limits separately (for example, with `oci limits resource-availability get` or the **Limits, Quotas and Usage** page in the Console). A launch succeeds only when both capacity and your limits allow it.
- **Results can vary by tenancy.** Allocation policies and placement constraints can make the same query return different results in different tenancies.
- **Only non-GPU ARM and AMD VM shapes are covered**, both standard and DenseIO. The script skips GPU, bare metal, Intel, and `.Generic` shapes, and doesn't request burstable or preemptible capacity.

### When you share results

Include the limitations with the numbers, and keep these points in mind:

- **AD names are tenancy-specific.** OCI [randomizes availability domains by tenancy](https://docs.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm), so `AD-1` in one tenancy isn't necessarily the same data center as `AD-1` in another. The report prints short AD names, so compare per-AD figures only within one tenancy.
- **Treat the output as point-in-time operational data** for internal planning, or for sharing with the customer it concerns. Don't publish it.

For example, paste this note with the output:

> Capacity figures come from the OCI compute capacity report: a best-effort, point-in-time estimate, not a reservation or launch guarantee. Per-shape estimates are summed arithmetically and aren't necessarily launchable at the same time. They don't account for capacity reservations or our tenancy service limits. Values marked † were reported as available without a count; the actual number is unknown.

## Prerequisites

- **Python 3.8 or later.** The script uses only the standard library.
- **The OCI CLI** on your `PATH`. See [Installing the CLI](https://docs.oracle.com/iaas/Content/API/SDKDocs/cliinstall.htm).
- **An OCI CLI configuration file** (`~/.oci/config`, or the path in `$OCI_CLI_CONFIG_FILE`) with a profile that uses API key authentication or a session token. For a session-token profile (created with `oci session authenticate`), pass `--auth security_token`, or set `OCI_CLI_AUTH=security_token`.
- **A subscription to the region** you query.
- **IAM policies** for the group that runs the report:

  ```text
  Allow group <your-group> to manage compute-capacity-reports in tenancy
  Allow group <your-group> to inspect instance-family in tenancy
  ```

  The first statement allows the capacity report. The `manage` verb is required; `use` isn't enough. The second statement allows the script to list shapes. Without these policies, the API returns `NotAuthorizedOrNotFound`.

## Run the report

Run the default sweep in your profile's region:

```bash
python3 oci_capacity_report.py
```

Query a specific region. Multi-AD regions are probed in every AD and consolidated:

```bash
python3 oci_capacity_report.py --region us-ashburn-1
```

Add the per-AD, per-shape-family breakdown, which helps you choose a shape:

```bash
python3 oci_capacity_report.py --region us-ashburn-1 --detail
```

Check only the node sizes that you plan to use:

```bash
python3 oci_capacity_report.py --region us-ashburn-1 --sizes 8,32
```

Check one exact configuration in every shape and every AD:

```bash
python3 oci_capacity_report.py --region us-ashburn-1 --ocpus 32 --memory-gb 512
```

List the discovered ARM and AMD shapes, their processors, vCPUs per OCPU, and published limits:

```bash
python3 oci_capacity_report.py --region us-ashburn-1 --show-shapes
```

Restrict the report to one AD. Use the short name that the report prints, such as `AD-2`, or the full AD name:

```bash
python3 oci_capacity_report.py --region us-ashburn-1 --ad AD-2
```

Use a session-token profile instead of an API key:

```bash
python3 oci_capacity_report.py --profile <session-profile> --auth security_token
```

A full sweep makes many API calls (sizes × shapes × ADs, three shapes per call). If OCI throttles a call (HTTP 429), the script retries it up to three times with increasing waits. To make fewer calls on a busy tenancy, narrow `--sizes` or use `--ad`.

### Options

| Option | Default | Description |
|---|---|---|
| `--region` | The profile's region | OCI region to query, such as `us-ashburn-1`. |
| `--profile` | `DEFAULT` | Profile in the OCI CLI configuration file. |
| `--auth` | CLI default | OCI CLI authentication method, passed as `--auth`. Use `security_token` for a session-token profile. `$OCI_CLI_AUTH` also works. |
| `--ad` | All ADs | Restrict the report to one availability domain, such as `AD-2` (or the full AD name). |
| `--sizes` | `1,2,4,8,16,32,64` | OCPU sizes to sweep. |
| `--arm-ratio` | `8` | GB of memory per OCPU for ARM rows. |
| `--amd-ratio` | `16` | GB of memory per OCPU for AMD rows. |
| `--ocpus N --memory-gb M` | — | Fit check: one exact configuration, every shape, every AD. |
| `--detail` | Off | In multi-AD regions, add a per-AD line with each shape family's estimate. |
| `--show-shapes` | Off | Print the discovered shapes and their limits, then exit. |
| `--tenancy-ocid` | From the profile | Root tenancy OCID override. |
| `--batch-size` | `3` | Shape entries per capacity-report request. Lower it if the API rejects requests for size. |

## Read the output

- **Estimate** is the arithmetic sum of OCI's per-shape estimates at that exact size (and across ADs in multi-AD regions). It isn't necessarily launchable at the same time.
- **Per-AD** (multi-AD regions) or **Per-family** (single-AD regions) shows where the total comes from.
- **The `families:` header lists every family discovered for that section in the selected ADs**, including families that don't fit every row. Only shapes whose limits fit a row's exact size count toward it. For example, fixed shapes such as `E2.1` rarely fit, and a shape that OCI rejects at that size shows as `invalid-cfg`. Run with `--detail` to see which families make up each total.
- **Rows are alternative scenarios.** Don't add rows together.
- **Fixed shapes appear only at their exact size.** A fixed shape, such as `VM.Standard.E2.1`, fits a row only when the row's OCPU count and memory match it exactly, so it usually doesn't appear in the sweep. Use `--show-shapes` to see fixed shapes, and the fit check (`--ocpus N --memory-gb M`) with their exact size to query them.

| Marker or status | Meaning |
|---|---|
| A number | OCI's estimate of how many nodes of this configuration it can launch now. |
| `OUT_OF_HOST_CAPACITY` or `0` | The configuration is valid, but OCI estimates no capacity now. |
| `avail†` or `N+†` | OCI reports capacity as available but didn't return a count for at least one shape. `N+†` means at least N. |
| `invalid-cfg` or `INVALID_CONFIG` | OCI rejects this configuration for that shape. Some shapes accept only specific sizes. |
| `not-supported` or `HARDWARE_NOT_SUPPORTED` | The shape isn't supported in that AD. |
| `—` | No discovered shape family supports that size in that location. |
| `N*` | Incomplete: at least one request failed or timed out. Run with `--detail` to see which one. |

### OCPUs and vCPUs

Sizes are in OCPUs, OCI's unit. How many vCPUs one OCPU provides depends on the processor, as described in [OCPUs and vCPUs](https://docs.oracle.com/en-us/iaas/Content/Compute/References/computeshapes.htm):

| Processor | Shape families | vCPUs per OCPU |
|---|---|---|
| Ampere Altra | A1 | 1 |
| AmpereOne | A2, A4 | 2 |
| x86 (AMD, Intel) | E3, E4, E5, E6, ... | 2 |

The script reads each shape's processor from ListShapes and groups shapes with the same ratio into one section, such as **ARM (1 vCPU/OCPU)**, **ARM (2 vCPU/OCPU)**, and **AMD (2 vCPU/OCPU)**. Every row in a section has one true vCPU size. If a new processor isn't recognized, the report shows `?` instead of guessing.

The memory ratios apply per OCPU. For example, the default ARM ratio of 8 GB per OCPU means 8 GB per vCPU on A1 but 4 GB per vCPU on A2 and A4.

To compare sections, match rows by vCPU and memory, not by OCPU. For example, A1 at 32 OCPU (32 vCPUs, 256 GB) is a comparable configuration to AMD at 16 OCPU (32 vCPUs, 256 GB). The fit check prints the comparable commands for you.

### Sample output

The following samples are **illustrative**. The numbers are made up, and they show the format only.

A tenancy that receives counts:

```text
Region : us-ashburn-1   ADs : AD-1, AD-2, AD-3 (all, consolidated)
Profile: DEFAULT

=== ARM (1 vCPU/OCPU) @ 1 OCPU : 8 GB   (region total across 3 ADs)   families: A1 ===
 OCPU/VM  vCPU/VM  RAM/VM(GB)      Estimate   Per-AD
------------------------------------------------------------------------------------------------
       8        8          64         7,440   (AD-1=2,400, AD-2=3,120, AD-3=1,920)
      32       32         256         1,860   (AD-1=600, AD-2=780, AD-3=480)

=== ARM (2 vCPU/OCPU) @ 1 OCPU : 8 GB   (region total across 3 ADs)   families: A2 ===
 OCPU/VM  vCPU/VM  RAM/VM(GB)      Estimate   Per-AD
------------------------------------------------------------------------------------------------
       8       16          64           372   (AD-1=120, AD-2=156, AD-3=96)
      32       64         256            93   (AD-1=30, AD-2=39, AD-3=24)

=== AMD (2 vCPU/OCPU) @ 1 OCPU : 16 GB   (region total across 3 ADs)   families: E4, E5 ===
 OCPU/VM  vCPU/VM  RAM/VM(GB)      Estimate   Per-AD
------------------------------------------------------------------------------------------------
       8       16         128         4,650   (AD-1=1,500, AD-2=1,950, AD-3=1,200)
      32       64         512         1,162   (AD-1=375, AD-2=487, AD-3=300)
...
Limitations: best-effort, point-in-time estimate from OCI's capacity report;
  not a reservation or launch guarantee. It does not consider capacity
  reservations or tenancy service limits/quotas (check those separately), and
  results can vary by tenancy.
```

A tenancy that receives the status only (exit code `3`):

```text
=== ARM (1 vCPU/OCPU) @ 1 OCPU : 8 GB   (region total across 3 ADs)   families: A1 ===
 OCPU/VM  vCPU/VM  RAM/VM(GB)      Estimate   Per-AD
------------------------------------------------------------------------------------------------
       8        8          64        avail†   (AD-1=avail†, AD-2=avail†, AD-3=avail†)
      32       32         256        avail†   (AD-1=avail†, AD-2=avail†, AD-3=avail†)
...
†  NOTE: OCI reported AVAILABLE but returned NO COUNT for some shapes.
   Depending on your tenancy's access level, OCI may return availability
   without a count. Totals marked † are lower bounds; the actual number
   is unknown.
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Every request returned a definite answer with a count. |
| `3` | Every request returned a definite status, but OCI didn't return a count for at least one available shape. Totals are lower bounds. |
| `2` | Coverage is incomplete: at least one request failed, timed out, or returned no usable row. |
| `1` | Setup error, such as a missing configuration, failed authorization, or bad arguments. |

## How it works

1. The script lists the region's availability domains (`oci iam availability-domain list`).
2. For each AD, it lists the VM shapes, their processors, and their published OCPU and memory limits (`oci compute shape list`). It keeps non-GPU ARM and AMD VM shapes (standard and DenseIO), classifying Ampere shapes as ARM and AMD shapes as AMD, and derives vCPUs per OCPU from the processor. It skips GPU, bare metal, Intel, and `.Generic` shapes.
3. For each size, it keeps only the shapes whose limits allow that configuration.
4. It calls `oci compute compute-capacity-report create` with up to three shapes per request. If the API rejects a request because one configuration is invalid, the script retries each shape on its own so that the other shapes still return results.
5. It sums the estimates per group, per AD, and per region, and prints the report.

The script is read-only. It doesn't create instances, reservations, or any other resources.

## Run the tests

The tests mock the OCI CLI, so they run without OCI access. The files in `tests/fixtures/` keep the OCI CLI's exact output format: real `shape list` rows, and a capacity-report response with illustrative counts.

```bash
python3 -m pip install pytest
python3 -m pytest tests/
```
