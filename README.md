<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" height="150px" srcset="https://raw.githubusercontent.com/lab-rasool/MINDS/main/docs/assets/README_logo.png">
        <img alt="logo" height="150px" src="docs/logo.png">
    </picture>
    <br>
    <h1>
    </h1>
</div>
<p align="center">
    <a href="https://pypi.org/project/med-minds/">
        <img src="https://img.shields.io/pypi/v/med-minds.svg" alt="PyPI Version">
    </a>
    <a href="https://pepy.tech/projects/med-minds">
        <img src="https://static.pepy.tech/badge/med-minds" alt="PyPI Downloads">
    </a>
    <a href="https://pypi.org/project/med-minds/">
        <img src="https://img.shields.io/pypi/pyversions/med-minds.svg" alt="Python Versions">
    </a>
    <a href="https://doi.org/10.3390/s24051634">
        <img src="https://img.shields.io/badge/DOI-10.3390%2Fs24051634-blue" alt="DOI">
    </a>
</p>

<!-- 115,974  -->

<!-- Make a dropdown tab -->

MINDS is a framework designed to integrate multimodal oncology data. It queries and integrates data from multiple sources, including clinical data, genomic data, and imaging data from the NIH NCI CRDC and IDC (Imaging Data Commons) portals.

> [!IMPORTANT]
> **TCIA Migration Notice:** As of this version, MINDS has migrated from TCIA (The Cancer Imaging Archive) to IDC (Imaging Data Commons) v2 API for imaging data access. TCIA is no longer hosting controlled-access data due to [new NIH policies](https://www.cancerimagingarchive.net/new-nih-policies-for-controlled-access-data/). All imaging data is now sourced from IDC, which provides broader access to imaging collections.

> [!NOTE]
> We are currently updating MINDS to include more data sources and improve the user experience. If you have any suggestions or would like to contribute, please feel free to reach out to us. Here is a list of the projects to be included in MINDS (115,974 total patients).
> <details>
> <summary>Projects in MINDS</summary>
> 
> | Project Name | Cases | Clinical | Radiology | Histopathology | Molecular |
> |--------------|-------|----------|-----------|----------------|-----------|
> | Foundation Medicine (FM) | 18,004 | ✓ | | | ✓ |
> | The Cancer Genome Atlas (TCGA) | 11,428 | ✓ | ✓ | ✓ | ✓ |
> | Therapeutically Applicable Research to Generate Effective Treatments (TARGET) | 6,543 | ✓ | | | ✓ |
> | Clinical Proteomic Tumor Analysis Consortium (CPTAC) | 1,656 | ✓ | ✓ | | ✓ |
> | The Molecular Profiling to Predict Response to Treatment (MP2PRT) | 1,562 | ✓ | | | ✓ |
> | Multiple Myeloma Research Foundation (MMRF) | 995 | ✓ | | | ✓ |
> | BEATAML1.0 | 882 | ✓ | | | ✓ |
> | Cancer Genome Characterization Initiatives (CGCI) | 645 | ✓ | | ✓ | ✓ |
> | NCI Center for Cancer Research (NCICCR) | 489 | ✓ | | | ✓ |
> | REBC | 449 | ✓ | | | ✓ |
> | MATCH | 448 | ✓ | | | ✓ |
> | Ukrainian National Research Center for Radiation Medicine Trio Study (TRIO) | 339 | ✓ | | | ✓ |
> | Count Me In (CMI) | 299 | ✓ | | | ✓ |
> | Human Cancer Model Initiative (HCMI) | 278 | ✓ | | ✓ | ✓ |
> | West Coast Prostrate Cancer Dream Team (WCDT) | 101 | ✓ | | | ✓ |
> | Oregon Health and Science University (OHSU) | 176 | ✓ | | | ✓ |
> | Applied Proteogenomics OrganizationaL Learning and Outcomes (APOLLO) | 87 | ✓ | | | ✓ |
> | EXCEPTIONAL RESPONDERS | 84 | ✓ | | | ✓ |
> | Environment And Genetics in Lung Cancer Etiology (EAGLE) | 50 | ✓ | | | ✓ |
> | ORGANOID | 70 | ✓ | | | ✓ |
> | Clinical Trials Sequencing Project (CTSP) | 45 | ✓ | | | ✓ |
> | VA Research Precision Oncology Program (VAREPOP) | 7 | ✓ | | | ✓ |
> | 4D-Lung | 20 | | ✓ | | |
> | A091105 | 83 | | ✓ | | |
> | AAPM-RT-MAC | 55 | | ✓ | | |
> | ACNS0332 | 85 | | ✓ | | |
> | ACRIN-6698 | 385 | | ✓ | | |
> | ACRIN-Contralateral-Breast-MR | 984 | | ✓ | | |
> | ACRIN-DSC-MR-Brain | 123 | | ✓ | | |
> | ACRIN-FLT-Breast | 83 | ✓ | ✓ | | |
> | ACRIN-FMISO-Brain | 45 | | ✓ | | |
> | ACRIN-HNSCC-FDG-PET-CT | 260 | | ✓ | | |
> | ACRIN-NSCLC-FDG-PET | 242 | | ✓ | | |
> | Adrenal-ACC-Ki67-Seg | 53 | ✓ | ✓ | | |
> | Advanced-MRI-Breast-Lesions | 632 | | ✓ | ✓ | ✓ |
> | AHEP0731 | 80 | | ✓ | | |
> | AHOD0831 | 165 | | ✓ | | |
> | AML-Cytomorphology_LMU | 200 | | | ✓ | |
> | AML-Cytomorphology_MLL_Helmholtz | 189 | | | ✓ | |
> | Anti-PD-1_Lung | 46 | | ✓ | | |
> | Anti-PD-1_MELANOMA | 47 | | ✓ | | |
> | APOLLO-5 | 414 | | ✓ | | |
> | ARAR0331 | 108 | | ✓ | | |
> | AREN0532 | 544 | | ✓ | | |
> | AREN0533 | 294 | | ✓ | | |
> | AREN0534 | 239 | | ✓ | | |
> | B-mode-and-CEUS-Liver | 120 | | ✓ | | |
> | Bone-Marrow-Cytomorphology_MLL_Helmholtz_Fraunhofer | 945 | | | ✓ | |
> | Brain-TR-GammaKnife | 47 | | ✓ | | |
> | Brain-Tumor-Progression | 20 | | ✓ | | |
> | Breast-Cancer-Screening-DBT | 5,060 | | ✓ | | |
> | BREAST-DIAGNOSIS | 88 | | ✓ | | |
> | Breast-Lesions-USG | 256 | | ✓ | | |
> | Breast-MRI-NACT-Pilot | 64 | | ✓ | | |
> | Burdenko-GBM-Progression | 180 | | ✓ | | |
> | C-NMC 2019 | 118 | | | ✓ | |
> | C4KC-KiTS | 210 | | ✓ | | |
> | CALGB50303 | 155 | | ✓ | | |
> | CBIS-DDSM | 1,566 | | ✓ | | |
> | CC-Radiomics-Phantom | 17 | | ✓ | | |
> | CC-Radiomics-Phantom-2 | 251 | | ✓ | | |
> | CC-Tumor-Heterogeneity | 23 | | ✓ | | |
> | CDD-CESM | 326 | | ✓ | | |
> | CMB-AML | 8 | | ✓ | ✓ | |
> | CMB-CRC | 49 | | ✓ | ✓ | |
> | CMB-GEC | 7 | | ✓ | ✓ | |
> | CMB-LCA | 61 | | ✓ | ✓ | |
> | CMB-MEL | 44 | | ✓ | ✓ | |
> | CMB-MML | 64 | | ✓ | ✓ | |
> | CMB-PCA | 12 | | ✓ | ✓ | |
> | CMMD | 1,775 | ✓ | ✓ | | ✓ |
> | CODEX imaging of HCC | 15 | | | ✓ | |
> | Colorectal-Liver-Metastases | 197 | | ✓ | | |
> | COVID-19-AR | 105 | | ✓ | | |
> | COVID-19-NY-SBU | 1,384 | | ✓ | | |
> | CRC_FFPE-CODEX_CellNeighs | 35 | | | ✓ | |
> | CT COLONOGRAPHY | 825 | ✓ | ✓ | | |
> | CT Images in COVID-19 | 661 | | ✓ | | |
> | CT Lymph Nodes | 176 | | ✓ | | |
> | CT-ORG | 140 | | ✓ | | |
> | CT-Phantom4Radiomics | 1 | | ✓ | | |
> | CT-vs-PET-Ventilation-Imaging | 20 | | ✓ | | |
> | CTpred-Sunitinib-panNET | 38 | | ✓ | | |
> | DFCI-BCH-BWH-PEDs-HGG | 61 | | ✓ | | |
> | DLBCL-Morphology | 209 | | | ✓ | |
> | DRO-Toolkit | 32 | | ✓ | | |
> | Duke-Breast-Cancer-MRI | 922 | | ✓ | | |
> | EA1141 | 500 | | ✓ | | |
> | ExACT | 30 | | ✓ | | |
> | FDG-PET-CT-Lesions | 900 | | ✓ | | |
> | GammaKnife-Hippocampal | 390 | | ✓ | | |
> | GBM-DSC-MRI-DRO | 3 | | ✓ | | |
> | GLIS-RT | 230 | | ✓ | | |
> | HCC-TACE-Seg | 105 | | ✓ | | |
> | HE-vs-MPM | 12 | | | ✓ | |
> | Head-Neck Cetuximab | 111 | | ✓ | | |
> | Head-Neck-PET-CT | 298 | | ✓ | | |
> | HEAD-NECK-RADIOMICS-HN1 | 137 | | ✓ | | |
> | Healthy-Total-Body-CTs | 30 | | ✓ | | |
> | HER2 tumor ROIs | 273 | | | ✓ | |
> | HistologyHSI-GB | 13 | | | ✓ | |
> | HNC-IMRT-70-33 | 211 | | ✓ | | |
> | HNSCC | 627 | | ✓ | | |
> | HNSCC-3DCT-RT | 31 | | ✓ | | |
> | HNSCC-mIF-mIHC-comparison | 8 | | | ✓ | |
> | Hungarian-Colorectal-Screening | 200 | | | ✓ | |
> | ISPY1 | 222 | | ✓ | | |
> | ISPY2 | 719 | | ✓ | | |
> | IvyGAP | 39 | | ✓ | | |
> | LCTSC | 60 | | ✓ | | |
> | LDCT-and-Projection-data | 299 | | ✓ | | |
> | LGG-1p19qDeletion | 159 | | ✓ | | |
> | LIDC-IDRI | 1,010 | | ✓ | | |
> | Lung Phantom | 1 | | ✓ | | |
> | Lung-Fused-CT-Pathology | 6 | | | ✓ | |
> | Lung-PET-CT-Dx | 355 | | ✓ | | |
> | LungCT-Diagnosis | 61 | | ✓ | | |
> | Meningioma-SEG-CLASS | 96 | | ✓ | | |
> | MIDRC-RICORD-1A | 110 | | ✓ | | |
> | MIDRC-RICORD-1B | 117 | | ✓ | | |
> | MIDRC-RICORD-1C | 361 | | ✓ | | |
> | MiMM_SBILab | 5 | | | ✓ | |
> | NADT-Prostate | 37 | | | ✓ | |
> | NaF PROSTATE | 9 | | ✓ | | |
> | NLST | 26,254 | | ✓ | ✓ | |
> | NRG-1308 | 12 | | ✓ | | |
> | NSCLC Radiogenomics | 211 | | ✓ | | |
> | NSCLC-Cetuximab | 490 | | ✓ | | |
> | NSCLC-Radiomics | 422 | | ✓ | | |
> | NSCLC-Radiomics-Genomics | 89 | | ✓ | | |
> | NSCLC-Radiomics-Interobserver1 | 22 | | ✓ | | |
> | OPC-Radiomics | 606 | | ✓ | | |
> | Osteosarcoma-Tumor-Assessment | 4 | | | ✓ | |
> | Ovarian Bevacizumab Response | 78 | | | ✓ | |
> | Pancreas-CT | 82 | | ✓ | | |
> | Pancreatic-CT-CBCT-SEG | 40 | | ✓ | | |
> | PCa_Bx_3Dpathology | 50 | ✓ | | ✓ | |
> | Pediatric-CT-SEG | 359 | | ✓ | | |
> | Pelvic-Reference-Data | 58 | | ✓ | | |
> | Phantom FDA | 7 | | ✓ | | |
> | Post-NAT-BRCA | 64 | | | ✓ | |
> | Pretreat-MetsToBrain-Masks | 200 | ✓ | ✓ | | |
> | Prostate Fused-MRI-Pathology | 28 | | | ✓ | |
> | Prostate-3T | 64 | | ✓ | | |
> | Prostate-Anatomical-Edge-Cases | 131 | | ✓ | | |
> | PROSTATE-DIAGNOSIS | 92 | | ✓ | | |
> | PROSTATE-MRI | 26 | | | ✓ | |
> | Prostate-MRI-US-Biopsy | 1,151 | | ✓ | | |
> | PROSTATEx | 346 | | ✓ | | |
> | Pseudo-PHI-DICOM-Data | 21 | | ✓ | | |
> | PTRC-HGSOC | 174 | | | ✓ | |
> | QIBA CT-1C | 1 | | ✓ | | |
> | QIBA-CT-Liver-Phantom | 3 | | ✓ | | |
> | QIN Breast DCE-MRI | 10 | | ✓ | | |
> | QIN GBM Treatment Response | 54 | | ✓ | | |
> | QIN LUNG CT | 47 | | ✓ | | |
> | QIN PET Phantom | 2 | | ✓ | | |
> | QIN PROSTATE | 22 | | ✓ | | |
> | QIN-BRAIN-DSC-MRI | 49 | | ✓ | | |
> | QIN-BREAST | 67 | | ✓ | | |
> | QIN-BREAST-02 | 13 | | ✓ | | |
> | QIN-HEADNECK | 279 | | ✓ | | |
> | QIN-PROSTATE-Repeatability | 15 | | ✓ | | |
> | QIN-SARCOMA | 15 | | ✓ | | |
> | RADCURE | 3,346 | ✓ | ✓ | | |
> | REMBRANDT | 130 | | ✓ | | |
> | ReMIND | 114 | | ✓ | | |
> | RHUH-GBM | 40 | | ✓ | | |
> | RIDER Breast MRI | 5 | | ✓ | | |
> | RIDER Lung CT | 32 | | ✓ | | |
> | RIDER Lung PET-CT | 244 | | ✓ | | |
> | RIDER NEURO MRI | 19 | | ✓ | | |
> | RIDER PHANTOM MRI | 10 | | ✓ | | |
> | RIDER PHANTOM PET-CT | 20 | | ✓ | | |
> | RIDER Pilot | 8 | | ✓ | | |
> | S0819 | 1,299 | | ✓ | | |
> | SLN-Breast | 78 | | | ✓ | |
> | SN-AM | 60 | | | ✓ | |
> | Soft-tissue-Sarcoma | 51 | | ✓ | | |
> | SPIE-AAPM Lung CT Challenge | 70 | | ✓ | | |
> | StageII-Colorectal-CT | 230 | | ✓ | | |
> | UCSF-PDGM | 495 | | ✓ | | |
> | UPENN-GBM | 630 | | | ✓ | |
> | Vestibular-Schwannoma-MC-RC | 124 | | ✓ | | |
> | Vestibular-Schwannoma-SEG | 242 | | ✓ | | |
> | VICTRE | 2,994 | | ✓ | | |
> </details>

## Installation

Currently the cloud version of MINDS is in closed beta, but, you can still recreate the MINDS database locally. To get the local version of the MINDS database running, you will need to setup a PostgreSQL database and populate it with the MINDS schema. This can be easily done using a docker container. First, you will need to install docker. You can find the installation instructions for your operating system [here](https://docs.docker.com/get-docker/). Next, you will need to pull the PostgreSQL docker image and run a container with the following command.

> [!NOTE]
> Please replace `my-secret-pw` with your desired password and `port` with the port you want to use to access the database. The default port for PostgreSQL is 5432. The following command will not work until you replace `port` with a valid port number.

```bash
docker run -d --name minds -e POSTGRES_PASSWORD=my-secret-pw -e POSTGRES_DB=minds -p port:5432 postgres
```

Finally, to install the MINDS python package use the following pip command:

```bash
pip install med-minds
```

After installing the package, please create a .env file in the root directory of the project with the following variables:

```bash
HOST=127.0.0.1
PORT=5432
DB_USER=postgres
PASSWORD=my-secret-pw
DATABASE=minds   
```

## PostgreSQL Migration (v0.0.6)

Version 0.0.6 introduces a migration from MySQL to PostgreSQL as the database backend, offering:

- Better performance for complex queries
- Advanced data types and indexing options
- More robust transaction support
- Better standards compliance

If you're upgrading from a previous version that used MySQL, please ensure your database environment is updated to use PostgreSQL 12 or later.

## Usage

### Initial setup and automated updates

If you have locally setup the MINDS database, then you will need to populate it with data. To do this, or to update the database with the latest data, you can use the following command:

```python
# Import the med_minds package
import med_minds

# Update the database with the latest data
med_minds.update()
```

### Querying the MINDS database

The MINDS python package provides a python interface to the MINDS database. You can use this interface to query the database and return the results as a pandas dataframe.

```python
import med_minds

# get a list of all the tables in the database
tables = med_minds.get_tables()

# get a list of all the columns in a table
columns = med_minds.get_columns("clinical")

# Query the database directly
query = "SELECT * FROM clinical WHERE project_project_id = 'TCGA-LUAD' LIMIT 10"
df = med_minds.query(query)
```

### Building the cohort and downloading the data

```python
# Generate a cohort to download from query
query_cohort = med_minds.build_cohort(query=query, output_dir="./data")

# or you can now directly supply a cohort from GDC
gdc_cohort = med_minds.build_cohort(gdc_cohort="cohort_Unsaved_Cohort.2024-02-12.tsv", output_dir="./data")

# to get the cohort details
gdc_cohort.stats()

# to download the data from the cohort to the output directory specified
# you can also specify the number of threads to use and the modalities to exclude or include
gdc_cohort.download(threads=12, exclude=["Slide Image"])
```

## Please cite our work

```bibtex
@Article{s24051634,
    AUTHOR = {Tripathi, Aakash and Waqas, Asim and Venkatesan, Kavya and Yilmaz, Yasin and Rasool, Ghulam},
    TITLE = {Building Flexible, Scalable, and Machine Learning-Ready Multimodal Oncology Datasets},
    JOURNAL = {Sensors},
    VOLUME = {24},
    YEAR = {2024},
    NUMBER = {5},
    ARTICLE-NUMBER = {1634},
    URL = {https://www.mdpi.com/1424-8220/24/5/1634},
    ISSN = {1424-8220},
    DOI = {10.3390/s24051634}
}
```

## Contributing

We welcome contributions from the community. If you would like to contribute to the MINDS project, please read our [contributing guidelines](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
