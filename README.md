# Vancouver Datajam 2023 workshop

## Automating ETL Processes with JupySQL and GitHub Actions

### Introduction

Data analytics and business intelligence rely heavily on efficient data extraction, transformation, and loading (ETL) processes. This workshop will provide participants with a comprehensive understanding of ETL and JupySQL - a Python library that enables seamless SQL based ETL from Jupyter notebooks. We will also introduce GitHub actions for scheduling and automating ETL processes. By the end of this workshop, participants will have hands-on experience with these tools and will be able to schedule their own ETL jobs.

### Objectives

By the end of this workshop, participants will know how to:

* Extract, transform, and load data using JupySQL, DuckDB and Amazon Redshift - from Jupyter notebooks.
* Automate and schedule ETL processes using GitHub Actions.
* Use Python and SQL to clean, aggregate, and transform data.
* Apply these skills to a real-world data management problem.

### Workshop Schedule

Duration: 2.5 hours

* Introductions
* **Section 1**: Introduction to ELT  (30 minutes)
* **Section 2**: Data extraction, wrangling and loading with SQL, JupySQL and DuckDB/Amazon Redshift (30 minutes)
* *Short Break* (15 minutes)
* **Section 3**: Introduction to GitHub actions (30 minutes)
* **Section 4**: CI/CD of ETL Processes with GitHub Actions (15 minutes)
* **Section 5**: Regularly fetch and populate a data with ELT and GitHub actions (15 minutes)

### Setup Instructions

1. Clone repository: 

```
git clone https://github.com/Vancouver-Datajam/automate-etl-github-actions.git
cd automate-etl-github-actions
```

2. Create a virtual environment and install dependencies:

```
conda create -n automate-etl python=3.10
conda activate automate-etl
pip install poetry
poetry install
```

### Pre-requisites

Participants should have:
* Basic and intermediate understanding of Python programming.
* Familiarity with SQL and Jupyter Notebooks.
* Installed Jupyter notebooks on their local systems.
* GitHub account.

### Speaker bio

Laura Funderburk works as a developer advocate for Ploomber. She has over three years of professional working experience in data science roles in a variety of settings including the private and the NGO sectors. Laura completed her B.Sc. Mathematics at SFU. In recognition of her ability to face adversity and give back to the community she forms part of, her Alma Mater awarded her a Terry Fox gold medal in 2019.