# Big Data Correlation Estimation

This project focuses on estimating covariance correlations in large-scale financial data, specifically using Countries ETF (Exchange-Traded Fund) data.

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Directory Structure](#directory-structure)
3. [Setup Instructions](#setup-instructions)
4. [Data Sources](#data-sources)
5. [Dependencies](#dependencies)


---

## Project Overview
This project involves processing and analyzing ETF data to estimate correlations and covariance matrices. The project is implemented in Python, with Jupyter Notebooks (`main.ipynb`) and helper functions (`helpers.py`).

---

## Directory Structure
The project is organized as follows:

```
project/
│
├── covariance/
│   └── covs_matrix.parquet
│
├── Data/
│   ├── average_diffs_seconds.csv
│   ├── clean/
│   │   ├── EDEN.parquet
│   │   ├── EFNL.parquet
│   │   ├── EIS.parquet
│   │   ├── EUSA.parquet
│   │   ├── EWA.parquet
│   │   ├── EWC.parquet
│   │   ├── EWD.parquet
│   │   ├── EWG.parquet
│   │   ├── EWH.parquet
│   │   ├── EWI.parquet
│   │   ├── EWJ.parquet
│   │   ├── EWK.parquet
│   │   ├── EWL.parquet
│   │   ├── EWN.parquet
│   │   ├── EWO.parquet
│   │   ├── EWP.parquet
│   │   ├── EWQ.parquet
│   │   ├── EWS.parquet
│   │   ├── EWT.parquet
│   │   ├── EWU.parquet
│   │   ├── EWW.parquet
│   │   ├── EWY.parquet
│   │   ├── EWZ.parquet
│   │   ├── INDA.parquet
│   │   └── MCHI.parquet
│
├── ETFs/
│   ├── ETFs-2007.tar
│   ├── ETFs-2008.tar
│   ├── ETFs-2009.tar
│   ├── ETFs-2010.tar
│   ├── ETFs-2011.tar
│   └── ETFs-2012.tar
│
├── main.ipynb
└── helpers.py
```

---

## Setup Instructions
1. **Clone the Repository**:
    ```bash
    git clone https://github.com/your-username/Big-Data-Correlation-Estimation.git
    cd Big-Data-Correlation-Estimation
    ```

2. **Download Data**: 
    Download the data from [here](https://epflch-my.sharepoint.com/:f:/g/personal/marco_giuliano_epfl_ch/Em55qhuee0lCl_JVPt6oySUBSbI7H1U7N5mllwGwEMvskw?e=Nti7E1).

3. **Organize the Directory**: 
    Place the downloaded files in the appropriate directories as shown in the [Directory Structure](#directory-structure).

4. **Install Dependencies**: 
    Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

---

## Data Sources

- **Clean Data**: This directory contains preprocessed ETF data in `.parquet` format, ready for analysis.
- **Covariance Data**: This directory includes the precomputed covariance matrices stored in `covs_matrix.parquet`.
- **ETF Data**: This directory holds the raw ETF data for the years 2007–2012, stored in `.tar` files. (If you can't download this, most of the notebook is still runnable)

---

## Dependencies
The project requires the following Python libraries:

- `pandas`
- `numpy`
- `pyarrow` (for `.parquet` file handling)
- `matplotlib` 
- `scipy` 
