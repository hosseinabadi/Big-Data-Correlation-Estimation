# Big-Data-Correlation-Estimation

## Project Directory Structure
Download clean data and covs_matrix.parquet from [here](https://epflch-my.sharepoint.com/:f:/g/personal/marco_giuliano_epfl_ch/Em55qhuee0lCl_JVPt6oySUBSbI7H1U7N5mllwGwEMvskw?e=Nti7E1).
The ETFs file are on the course [drive](https://drive.switch.ch/index.php/s/0X3Je6DauQRzD2r?path=%2FETFs).
Once downloaded organize your directory as this.

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