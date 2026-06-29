# 274-Only Provider Ingestion Dimension Pipeline

This repository contains the production-grade, declarative, configuration-driven ETL pipeline for processing EDI 274 (Healthcare Provider Directory) feeds. 
This project focuses **exclusively on the 274 directory feed**, projecting all 837 claim-dependent provider demographics, specialties, and bridge tables as `NULL` in the final Gold table.

The codebase is aligned to be identical in structure and orchestration flow to the parallel Member dimension project.

---

## 1. Project Directory Layout

```
claimprocessing_provider274/
├── DDL/
│   └── DimProvider/
│       ├── gold_dimprovider.sql
│       └── silver_provider_hierarchy.sql
├── DimProvider/
│   ├── Bronze/
│   │   └── Schema/
│   │       └── provider_hierarchy_7.12_schema.json
│   ├── EDIProcessing/
│   │   ├── __init__.py
│   │   ├── mapper.py
│   │   └── mappings.py
│   ├── Gold/
│   │   ├── Notebooks/
│   │   │   └── GenericSubGroupProcessing.ipynb
│   │   └── Schema/
│   │       └── dimProvider.json
│   ├── Silver/
│   │   └── Notebooks/
│   │       └── ProviderHierarchy.ipynb
│   ├── Provider_Pipeline.ipynb
│   └── ddl_executor.ipynb
├── Shared/
│   ├── CommonMethods/
│   │   └── Helpers/
│   │       ├── CreateUserDefinedFunctions.ipynb
│   │       ├── ErrorHandling.ipynb
│   │       ├── FileHandling.ipynb
│   │       └── SynJSONCreatorClass.ipynb
│   ├── EDIProcessing/
│   │   ├── __init__.py
│   │   └── csvconverter.py
│   │   └── ediprocessing.py
│   └── Notebooks/
│       ├── FilesToProcess.ipynb
│       └── MoveFileToProcess.ipynb
├── source/
│   └── 274/
│       └── pending/
└── temp/
```

---

## 2. Stage-wise Data Flow Diagram

```mermaid
graph TD
    %% STAGE 1
    subgraph STAGE_1 [Stage 1: EDI Parsing to CSV]
        A[source/274/pending/raw.txt] -->|Read & Parse| B(Shared.EDIProcessor)
        B -->|Structured JSON| C[DimProvider.Mapper / mappings.py]
        C -->|Declarative Flat Dict| D(Shared.CSVConverter)
        D -->|Write CSV| E[temp/274/provider_hierarchy.csv]
    end

    %% STAGE 2
    subgraph STAGE_2 [Stage 2: Bronze Ingestion]
        E -->|Read CSV| F[Shared.FilesToProcess.ipynb]
        G[provider_hierarchy_7.12_schema.json] -->|Validation Schema| F
        F -->|Write Parquet| H[Volume: bronze/processed_parquet/provider_hierarchy]
    end

    %% STAGE 3
    subgraph STAGE_3 [Stage 3: Silver Transformation & 1-to-3 Split]
        H -->|Load Bronze DataFrame| I[DimProvider.ProviderHierarchy.ipynb]
        J[silver.ref_provider_affiliation] -->|Join Group Details| I
        K[silver.ref_credentialing] -->|Join DoNotChase Flag| I
        I -->|1-to-3 Union Split SQL| L[Table: silver.provider_hierarchy]
    end

    %% STAGE 4
    subgraph STAGE_4 [Stage 4: Gold SCD Type 2 Merge]
        L -->|Load Silver DataFrame| M[DimProvider.GenericSubGroupProcessing.ipynb]
        N[dimProvider.json Config] -->|Mapping Instructions| M
        M -->|SCD Type 2 Merge SQL| O[Table: gold.dimprovider]
    end

    %% Lifecycle
    O -->|Success| P[Move raw.txt: pending -> processed]
    M -->|Failure| Q[Move raw.txt: pending -> failed]
```

---

## 3. Explanation of Organizational Tiers (Tier 1 to Tier 4)

In healthcare directory processing, providers and clinic groups are organized hierarchically:

* **Tier 1 (Practitioner Level)**: Represents the individual practitioner (the doctor, clinician) who performs the medical service. Key identifier is the individual doctor's NPI.
* **Tier 2 (Location / Practice Group Level)**: Represents the physical office, clinic location, or group practice where the doctor practices and that bills for the services. Key identifier is the Clinic NPI (`TIER2ID`) and billing Tax ID (`LOCATIONTIN`).
* **Tier 3 (Health System Level)**: The hospital network, parent organization, or corporate health system that owns or manages multiple Tier 2 practice locations (e.g. *Mercy Health System*). Resolved by joining the reference table `ref_provider_affiliation`.
* **Tier 4 (Payer Network Level)**: The highest corporate tier representing the payer network or specific insurance plan network (e.g. *Aetna Health*).

---

## 4. 274 Dimension Staging Columns & Definitions

Below is the definition of all **46 columns** processed in the Bronze and Silver staging layers:

| Column Name | Data Type | Description | Source EDI 274 Segment / Loop |
|---|---|---|---|
| `TEMPLATE` | string | CSV structure validation placeholder. | Static value `'TEMPLATE'` |
| `PROVIDERID` | string | Unique NPI of the rendering doctor. | `NM1*1P` Loop (uses `nm111_111` or `identifier`) |
| `PROVIDERLASTNAME` | string | Full name of the individual doctor. | `NM1*1P` Loop (concatenates first + middle + last name) |
| `PROVIDERNPI` | string | National Provider Identifier (NPI) of doctor. | `NM1*1P` Loop (`nm111_111` or `identifier`) |
| `LOCATIONGROUPID` | string | Group submitter ID or associated clinic NPI. | `NM1*41` Submitter ID or `NM1*85` Clinic NPI |
| `LOCATIONRANKING` | integer | Priority indicator of the location. | Static integer `1` |
| `LOCATIONIDTYPE` | string | Role of record: `rendering`, `billing`, or `rendering to billing`. | Calculated dynamically in Silver layer |
| `LOCATIONID` | string | ID of location: Doctor NPI (rendering) or TIN (billing/link). | `NM1*1P` NPI (Rendering) or `LOCATIONTIN` (Billing/Link) |
| `LOCATIONDESC` | string | Name of the location/practitioner profile. | Doctor Name or Clinic Name |
| `LOCATIONTIN` | string | Tax Identification Number (TIN) of clinic. | `REF*EI` or `REF*1H` (Employer ID / TIN) |
| `LOCATIONADDRESS1` | string | Primary street address of the doctor. | `N3` segment of child loop |
| `LOCATIONADDRESS2` | string | Secondary address of the doctor (e.g. Suite). | `N3` segment of child loop |
| `LOCATIONCITY` | string | Practice city of the doctor. | `N4` segment of child loop |
| `LOCATIONSTATE` | string | Practice state of the doctor. | `N4` segment of child loop |
| `LOCATIONZIP` | string | Practice zip code of the doctor. | `N4` segment of child loop |
| `COUNTYCODE` | string | County code placeholder. | Unmapped (`null`) |
| `PHONENUMBER` | string | Main phone number for provider/clinic. | `PER*AJ` or `PER*IC` (phone number) |
| `FAXNUMBER` | string | Main fax number placeholder. | Unmapped (`null`) |
| `CONTACTPERSON` | string | Name of contact person. | `PER*AJ` or `PER*IC` (contact name) |
| `DONOTCHASE` | string | Flag showing if provider credential check is bypassed. | Resolved in Silver via `ref_credentialing` |
| `TIER2IDTYPE` | string | Tier 2 ID Qualifier placeholder. | Unmapped (`null`) |
| `TIER2ID` | string | NPI of the Tier 2 Clinic Group. | Parent `NM1*85` NPI or `ref_provider_affiliation` |
| `TIER2DESC` | string | Name of the Tier 2 Clinic Group. | Parent `NM1*85` Name or `ref_provider_affiliation` |
| `TIER2ADDRESS1` | string | Primary address of the clinic. | Parent `N3` address or `ref_provider_affiliation` |
| `TIER2ADDRESS2` | string | Secondary address of the clinic. | Unmapped (`null`) |
| `TIER2CITY` | string | City of the clinic. | Parent `N4` city or `ref_provider_affiliation` |
| `TIER2STATE` | string | State of the clinic. | Parent `N4` state or `ref_provider_affiliation` |
| `TIER2ZIP` | string | Zip code of the clinic. | Parent `N4` zip or `ref_provider_affiliation` |
| `TIER3IDTYPE` | string | Tier 3 ID Qualifier placeholder. | Unmapped (`null`) |
| `TIER3ID` | string | Identifier of the Tier 3 Health System. | Resolved in Silver via `ref_provider_affiliation` |
| `TIER3DESC` | string | Name of the Tier 3 Health System. | Resolved in Silver via `ref_provider_affiliation` |
| `TIER3ADDRESS1` | string | Address of the Tier 3 entity. | Unmapped (`null`) |
| `TIER3ADDRESS2` | string | Secondary address of the Tier 3 entity. | Unmapped (`null`) |
| `TIER3CITY` | string | City of the Tier 3 entity. | Unmapped (`null`) |
| `TIER3STATE` | string | State of the Tier 3 entity. | Unmapped (`null`) |
| `TIER3ZIP` | string | Zip code of the Tier 3 entity. | Unmapped (`null`) |
| `TIER4IDTYPE` | string | Tier 4 ID Qualifier placeholder. | Unmapped (`null`) |
| `TIER4ID` | string | Identifier of the Tier 4 Payer network. | Unmapped (`null`) |
| `TIER4DESC` | string | Name of the Tier 4 Payer network. | Unmapped (`null`) |
| `TIER4ADDRESS1` | string | Address of the Tier 4 entity. | Unmapped (`null`) |
| `TIER4ADDRESS2` | string | Secondary address of the Tier 4 entity. | Unmapped (`null`) |
| `TIER4CITY` | string | City of the Tier 4 entity. | Unmapped (`null`) |
| `TIER4STATE` | string | State of the Tier 4 entity. | Unmapped (`null`) |
| `TIER4ZIP` | string | Zip code of the Tier 4 entity. | Unmapped (`null`) |
| `STARTDATE` | timestamp | Relationship effective start date. | `DTP*007` segment or transaction date in `BHT04` |
| `ENDDATE` | timestamp | Relationship effective end date. | `DTP*008` segment |
| `CLIENT_ID` | string | Submitter / sender ID for multi-tenant data partitioning. | `ISA06` (Interchange Sender ID) |
| `FILE_ID` | string | Interchange Control Number to identify the source file. | `ISA13` (Interchange Control Number) |
| `LOAD_DATETIME` | timestamp | System timestamp when the file was processed. | System-generated by Spark |
| `FILE_LAYOUT_ID` | string | Identifier of file layout (statically set to `'274'`). | Static Configuration |
| `FILE_LAYOUT_DESCRIPTION` | string | Description of file layout (statically set to `'Standard274'`). | Static Configuration |
| `HashKey` | string | Unique SHA-256 signature of the row content. | Spark-generated Hash of all columns |

### 4.1 Detailed Column Casing and Value Mapping Across the 3-Row Split

In the Silver layer, each parsed record is split into three distinct rows (`rendering`, `billing`, and `rendering to billing`). Below is the behavior of key columns across these three rows:

#### 1. Core Identifiers & Names
*   **`PROVIDERID`**:
    *   *Rendering Row*: Mapped to the Doctor's individual NPI (e.g. `1982736452`).
    *   *Billing Row*: Mapped to the Clinic Group's Tax ID / `LOCATIONTIN` (e.g. `123456789`).
    *   *Linkage Row*: Mapped to the Doctor's individual NPI.
*   **`PROVIDERLASTNAME`**:
    *   *Rendering Row*: Dr. John M. Doe (Doctor's combined Full Name).
    *   *Billing Row*: Johns Hopkins Hospital (coalesced with `TIER2DESC`).
    *   *Linkage Row*: Dr. John M. Doe.
*   **`PROVIDERNPI`**:
    *   *Rendering Row*: Doctor's NPI (e.g. `1982736452`).
    *   *Billing Row*: Organization's Group NPI (`TIER2ID` e.g. `1992837465`).
    *   *Linkage Row*: Doctor's NPI.
*   **`LOCATIONID`**:
    *   *Rendering Row*: Doctor's NPI.
    *   *Billing Row*: Clinic's Tax ID / `LOCATIONTIN` (e.g. `123456789`).
    *   *Linkage Row*: Clinic's Tax ID / `LOCATIONTIN` (e.g. `123456789`).
*   **`LOCATIONDESC`**:
    *   *Rendering Row*: Doctor's Name.
    *   *Billing Row*: Clinic Group Name.
    *   *Linkage Row*: Clinic Group Name.

#### 2. Office & Contact Details
*   **`LOCATIONTIN`**: Always holds the billing Tax ID (`123456789`) across all three rows for financial tracing.
*   **Addresses (`LOCATIONADDRESS1` etc.)**: 
    *   *Rendering Row*: Practice street address.
    *   *Billing & Linkage Rows*: Coalesced to use the Clinic Group's address (`TIER2ADDRESS1` etc.), falling back to the rendering address if missing.
*   **`LOCATIONRANKING`**: Statically set to `1` (indicating primary office location).
*   **`DONOTCHASE`**: Mapped dynamically in the Silver query by joining against `silver.ref_credentialing` on `PROVIDERID` (defaults to `'N'`).
*   **Contact Info**: `PHONENUMBER`, `FAXNUMBER`, and `CONTACTPERSON` are extracted from the `PER` segment and map to office administrative numbers.

#### 3. Organizational Tiers (Tiers 2-4)
*   **`TIER2ID` / `TIER2DESC` (Clinic Group)**: Resolved via `ref_provider_affiliation` or the parent `NM1*85` segment. Represents the hospital/clinic group organization (e.g. Johns Hopkins Hospital, NPI `1992837465`).
*   **`TIER3ID` / `TIER3DESC` (Health System)**: Represents the overarching corporate parent system (e.g. Johns Hopkins Medicine). Resolved via `ref_provider_affiliation`.
*   **`TIER4ID` / `TIER4DESC` (Network/Payer)**: Represents the payer network (e.g. Aetna). Defaults to null.

#### 4. Audit & Change Tracking Metadata
*   **`CLIENT_ID` / `FILE_ID` / `LOAD_DATETIME`**: Extracted from interchange wrappers to track data lineage and multi-tenant plan partitions.
*   **`HashKey`**: A 64-character SHA-256 hash string generated dynamically across all row values. Used in downstream Gold merges to quickly detect changed records and execute SCD Type 2 upserts.

---

## 5. EDI 274 Envelope & Segment Reference Guide

Healthcare directory files contain structural envelope segments to bundle data safely, followed by transaction segments that map out the provider networks.

### 🌐 The Envelope Components (Universal Wrappers)
These segments are required for all EDI files, not just 274. They act like layers of an envelope to bundle the data safely between trading partners.
* **ISA (Interchange Control Header)**: The absolute outer envelope. It contains the sender ID, receiver ID, date, time, and security passwords. Think of it as the physical delivery box.
* **GS (Functional Group Header)**: The inner envelope that groups similar files together. In this file, it contains the code `HR`, which tells the parsing system: *"Everything inside this group is a Provider Information (274) file."*
* **GE (Functional Group Trailer)**: Closes out the GS inner envelope and contains a count of the transaction sets inside.
* **IEA (Interchange Control Trailer)**: Closes out the entire ISA outer box and verifies that no data was lost during transfer.

---

### 📄 The Transaction Specifications (Specific to EDI 274)
These segments build the internal structure, hierarchies, and specific medical credentials of the provider network.

| Segment ID | Segment Name | What it Specifies in the EDI 274 File |
|---|---|---|
| **ST** | Transaction Set Header | Marks the start of the actual 274 data layout and defines the specific HIPAA regulatory sub-version (e.g., `005010X292`). |
| **BHT** | Beginning of Hierarchical Transaction | Sets the operational purpose of the file (e.g., whether this is an original data load, an update, or a retransmission). |
| **HL** | Hierarchical Level | The core engine of the 274. It establishes the parent-child relationships that connect the Insurance Payer $\rightarrow$ Hospital Facility $\rightarrow$ Individual Doctor. |
| **NM1** | Individual or Organizational Name | Transmits the literal name of the entity in that loop (e.g., "Aetna", "Johns Hopkins Hospital", or "John Doe"). |
| **PER** | Administrative Communications Contact | Contains phone numbers, emails, and fax numbers for specific departments or personnel. |
| **N3** | Address Information | The street-level address lines for the practice locations or facilities. |
| **N4** | Geographic Location | The city, state, zip code, and country data for the physical location. |
| **REF** | Reference Information | Transmits critical legal identifiers like State License Numbers, Medicaid IDs, or Federal Tax IDs (EIN). |
| **PRV** | Provider Specialty / Taxonomy | Contains the healthcare provider taxonomy codes detailing their medical specialty (e.g., General Practice vs. Cardiology). |
| **DMG** | Demographic Information | Specific individual traits of a human provider, such as Date of Birth and Gender. |
| **LUI** | Language Indicator | Specifies languages spoken by the doctor or supported at the clinic location. |
| **HSD** | Health Care Delivery | Conveys scheduling parameters (e.g., open hours, days available, or whether they are taking new patients). |
| **TPB** | Third-Party Benefit | Explicitly defines which commercial insurance plans, HMO networks, or PPO products this doctor participates in. |
| **N1** | Name | Defines external parent companies, joint ventures, or Independent Physician Associations (IPAs) the doctor belongs to. |
| **ACT** | Account Cross-Reference Number | Tracks contracts, system account groupings, or vendor control numbers between platforms. |
| **NX1** | Real Estate / Property Location Pointer | A structural marker that says, *"The segments immediately following this specify traits for this specific branch office location."* |
| **EDU** | Educational Background | Identifies the medical school attended, degrees earned (MD, DO), and training credentials. |
| **DTP** | Date or Time Period | Tracks critical dates associated with other loops, such as Medical School Graduation Date or Board Certification Effective Date. |
| **LCC** | Licensing and Board Certification | Transmits official board certification statuses (e.g., American Board of Internal Medicine). |
| **SE** | Transaction Set Trailer | Marks the end of the 274 data segment array and includes a line count to verify file integrity. |

---

## 6. Reference Tables & Data Enrichment Strategy

In Stage 3 (Silver layer), the staging pipeline joins the Bronze directory record with two reference tables in your database:
1. **`silver.ref_provider_affiliation`**: Mapped using the clinic Tax ID (`LOCATIONTIN`).
2. **`silver.ref_credentialing`**: Mapped using the doctor NPI (`PROVIDERID`).

### Why are they needed?
* **Enriching Incomplete Files (Fallback Lookup)**: While large hospital groups submit complete files with Tier 2 (Clinic Group) names and Tier 3 (Health System) names, solo practitioners send files containing *only* their name, NPI, and Tax ID. To ensure every database record has its clinic group name, we use `ref_provider_affiliation` to look up and populate the missing descriptions based on their Tax ID.
* **Internal Business Flags**: The `DONOTCHASE` flag (which indicates if provider credential updates are bypassed) is managed internally by your credentialing team. This information is never sent inside the raw EDI 274 file. We join `ref_credentialing` on the doctor NPI to fetch and inject this clinical status.

---

## 7. Ingestion Isolation (Zero Dependency on 837 Claims)

This ingestion pipeline is designed to be **100% self-contained and isolated from the 837 claim ingestion process**. 
* In a combined system, columns like provider first/middle names, billing tax identifiers, or taxonomy qualifications are sometimes populated from the 837 claims feed.
* For this 274-only implementation, all such 837-dependent columns (such as `firstName`, `middleName`, `providerDEA`, `taxonomyCode1-5`, `hpSpecialtyCode1-5`, and `isContracted`) are projected as **`NULL`** in `dimProvider.json`:
  ```json
  CAST(NULL AS string) AS firstName,
  CAST(NULL AS string) AS middleName,
  CAST(NULL AS string) AS providerDEA,
  ...
  ```
This guarantees that the 274 directory feed runs completely independently without any dependency on active claims processing.

---

## 8. EDI 274 Multi-Level Organizational Hierarchy Structure

The EDI 274 format uses **Hierarchical Level (HL) segments** to represent organizational relationships. The `HL` segment defines the structure using:
* `HL01` (Hierarchical ID)
* `HL02` (Parent Hierarchical ID)
* `HL03` (Level Code: `20` = Source/Network, `21` = Group/Clinic, `22` = Individual Doctor)
* `HL04` (Child Code: `1` = has children, `0` = has no children)

---

### EXAMPLE 1: 2-Level Hierarchy (Individual Practitioner $\rightarrow$ Clinic Group)
This represents a standard relationship where an individual doctor renders services at a clinic location:

```
HL*1**20*1~                                   <-- Parent (Level ID: 1, Level Code: 20)
NM1*85*2*PROVIDER NAME*****XX*1234567893~     <-- Clinic Name & NPI (Tier 2)
N3*123 MAIN STREET~                           <-- Clinic Street Address
N4*CITY*STATE*ZIP*COUNTRY CODE~               <-- Clinic City, State, Zip
REF*EI*123456789~                             <-- Clinic Tax ID (TIN)
PER*IC*CONTACT NAME*TE*PHONE NUMBER~         <-- Clinic Contact details
PRV*BI*PXC*207Q00000X~                        <-- Clinic Taxonomy/Specialty
DMG*D8*19700101*M~                            <-- Demographics placeholder

HL*2*1*21*1~                                  <-- Child (Level ID: 2, Parent ID: 1, Level Code: 21)
NM1*1P*1*RELATED PROVIDER NAME*****XX*9876543210~ <-- Doctor Name & NPI (Tier 1)
N3*456 SECOND AVENUE~                         <-- Doctor Office Street Address
N4*CITY*STATE*ZIP*COUNTRY CODE~               <-- Doctor Office City, State, Zip
PRV*AT*PXC*208D00000X~                        <-- Doctor Taxonomy/Specialty
```

#### Mapping Breakdown:
1. **The Parent (`HL*1`)** establishes the **Tier 2 Clinic Group** details. 
   * Clinic Name (`PROVIDER NAME`), Clinic NPI (`1234567893`), and Clinic Address (`123 MAIN STREET`) are mapped directly to `TIER2DESC`, `TIER2ID`, and `TIER2ADDRESS1`.
   * The `REF*EI` segment provides the clinic's **Tax ID (`LOCATIONTIN`)**.
2. **The Child (`HL*2`)** establishes the **Tier 1 Doctor** details.
   * `HL*2*1` indicates that `HL*2` is a child of `HL*1`, linking the doctor to the clinic.
   * Doctor Name (`RELATED PROVIDER NAME`) and NPI (`9876543210`) are mapped to `PROVIDERLASTNAME` and `PROVIDERNPI`.

---

### EXAMPLE 2: 3-Level Hierarchy (Practitioner $\rightarrow$ Hospital $\rightarrow$ Network/Payer)
This represents a complex structure where an individual doctor practices at a hospital, which operates under a specific health network/payer contract:

```
ST*274*0001~
BGN*11*FILE20260627*20260627*145300******2~

hl*1**20*1~                                   <-- Top-Level Parent (Level ID: 1, Level Code: 20)
NM1*85*2*AETNA HEALTH*****PI*AETNA123~         <-- Network/Payer Name & ID (Tier 4)

HL*2*1*21*1~                                  <-- Mid-Level Child (Level ID: 2, Parent ID: 1, Level Code: 21)
NM1*1P*2*JOHNS HOPKINS HOSPITAL*****XX*1992837465~ <-- Hospital Group Name & NPI (Tier 2)
N3*600 N WOLFE ST~                            <-- Hospital Address
N4*BALTIMORE*MD*21287~

hl*3*2*22*0~                                  <-- Bottom-Level Child (Level ID: 3, Parent ID: 2, Level Code: 22)
NM1*1P*1*DOE*JOHN*M*DR**XX*1982736452~        <-- Individual Doctor Name & NPI (Tier 1)
PRV*PE*PXC*207Q00000X~                        <-- Doctor Taxonomy/Specialty
```

#### Mapping Breakdown:
1. **Level 1 Parent (`hl*1`)**: The top level represents **Aetna Health** (Payer Network). This maps to **Tier 4** (`TIER4ID` = `AETNA123`, `TIER4DESC` = `AETNA HEALTH`).
2. **Level 2 Child (`HL*2*1`)**: Linked to Level 1. Represents **Johns Hopkins Hospital**. This maps to **Tier 2** (`TIER2ID` = `1992837465`, `TIER2DESC` = `JOHNS HOPKINS HOSPITAL`, `TIER2ADDRESS1` = `600 N WOLFE ST`).
3. **Level 3 Child (`hl*3*2`)**: Linked to Level 2. Represents the individual practitioner, **Dr. John M Doe**. This maps to **Tier 1** (`PROVIDERNPI` = `1982736452`, `PROVIDERLASTNAME` = `JOHN M DOE`).

#### Database Resolution (1-to-3 Split):
* **Rendering Record**: Doctor `JOHN M DOE` (`1982736452`) at location address `600 N WOLFE ST`.
* **Billing Record**: Hospital `JOHNS HOPKINS HOSPITAL` (`1992837465`) at billing address `600 N WOLFE ST`.
* **Link Record**: Connects Doctor NPI `1982736452` to Hospital NPI `1992837465` under the Network Payer ID `AETNA123`.

---

## 9. How to Test in Databricks

1. In Databricks, pull the latest commits from the remote branch.
2. Run the **`ddl_executor`** notebook to create schemas and tables under catalog `274`.
3. Put your raw EDI 274 files under the volume path `/Volumes/274/bronze/processed_parquet/provider_hierarchy` or place them in `source/274/pending/`.
4. Run the orchestrator notebook **`Provider_Pipeline`** (with the widget `ClientContainer` set to `274`).
5. Verify that the files are moved to `processed` only after a successful Gold merge, and that the data is split into 3 rows correctly in `` `274`.silver.provider_hierarchy `` and `` `274`.gold.dimprovider ``.
