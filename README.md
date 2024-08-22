# CademyCode Subscription Pipeline

CademyCode Subscription Pipeline is a Python-based data processing pipeline designed to clean, merge, and update data stored in a SQLite database. The pipeline processes student, job, and course data, logs the operations performed, and updates versioning information with each run.

## Table of Contents

1. [Introduction](#introduction)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Configuration](#folderstructure)
6. [Testing](#testing)


## Introduction

CademyCode Subscription Pipeline automates the processing of educational data. It reads from a SQLite database, cleans and merges the data, updates version information, and logs all steps. The pipeline is designed to handle data related to students, their jobs, and courses they've taken.

- Used Jupyter notebook in the `\dev` folder for the initial exploration
- SQLAlchemy to establish a database connection
- Initial run on database `cademycode.db` for the development. To test it on the updated database, change database name in the pipeline.py to `cademycode_updated.db`


## Features

- **Data Cleaning**: Removes NaN values and duplicates from the data.
- **Database Update**: Automatically updates the SQLite database with cleaned and processed data.
- **Data Merging**: Merges student, job, and course data into a single dataset.
- **Versioning**: Tracks and updates the version of the pipeline with each run.
- **Logging**: Logs all operations performed by the pipeline for audit purposes.

## Installation

To set up the CademyCode Subscription Pipeline, follow these steps:

### Prerequisites

Ensure you have the following installed:

- Python 3.8+
- Required Python packages (listed in `requirements.txt`)
- SQLite (optional if not using `sqlite3` command-line tool)

### Steps

1. **Clone the repository**:

    ```bash
    git clone https://github.com/rkotkas/subscription-pipeline
    ```

2. **Navigate to the project directory**:

    ```bash
    cd subscription-pipeline
    ```

3. **Install the required Python packages**:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

The pipeline script `pipeline.py` can be run using the bash script `script.sh` to run tests, process the data and update the database. Script.sh first runs the unittests in `tests.py`, then creates the `\build` directory, copy `pipeline.py`  from `\dev` directory and run `pipeline.py`. Here’s how to use it:

### Running the Pipeline

```bash
./script.sh
```

## Folder Structure

Files:
- **script.sh**: bash script to move files to `\build` and run the data processor 
- **requirements.txt**
- **version.txt**: created after the first run, contains current version and is updated when updates found in the database
- **row_counts.json**: holds the current row count of the tables
- **changelog.log**: logs the info from the process steps 
- **errorlog.log**: log the errors from the process steps
- **cademycode.db**: database containing data from 3 tables:
    - `cademycode_students`
    - `cademycode_student_jobs`
    - `cademycode_courses`
- **cademycode_updated.db**: updated version of `cademycode.db` for testing the update process
- **README.md**

### dev
- pipeline.py: gets data from database `cademycode.db`, process it, updates database, merges final data into a CSV
- merged_data.csv: automatically generated merged dataframes after running `pipeline.py`

### build
- pipeline.py: copied from `\dev`
- merged_data.csv: automatically generated merged dataframes after running `pipeline.py`

### tests
- tests.py: unitests for `pipeline.py`
- testlog.log: log the info from unitests


## Testing
The unittests (`tests.py`) run before the code (`pipeline.py`)