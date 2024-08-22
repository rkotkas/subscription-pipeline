# CademyCode Subscription Pipeline

The CademyCode Subscription Pipeline is a Python-based data processing pipeline designed to clean, merge, and update data stored in a SQLite database. It processes student, job, and course data, logs operations, and updates versioning information with each run.

## Table of Contents

1. [Introduction](#introduction)
2. [Features](#features)
3. [Project Setup](#setup)
4. [Usage](#usage)
5. [Folder Structure](#folderstructure)
6. [Testing](#testing)

## Introduction

The CademyCode Subscription Pipeline automates the processing of educational data. It reads from a SQLite database, cleans and merges data, updates version information, and logs all operations.

Key components:

- Initial Exploration: Conducted using Jupyter Notebook.
- Database Connection: Managed with SQLAlchemy.
- Database Configuration: By default, uses cademycode.db. Update pipeline.py to use cademycode_updated.db for testing.

## Features

- **Data Cleaning**: Removes NaN values and duplicates.
- **Database Update**: Automatically updates the SQLite database with processed data.
- **Data Merging**: Combines student, job, and course data into a unified dataset.
- **Versioning**: Tracks and updates pipeline version with each run.
- **Logging**: Captures detailed logs for operations and errors.


## Project Setup

### Prerequisites

Ensure you have the following installed:

- Python 3.8+
- SQLite (optional, if not using `sqlite3` command-line tool)

Clone the repository:

    ```bash
    git clone https://github.com/rkotkas/subscription-pipeline
    ```

Navigate to the project directory:

    ```bash
    cd subscription-pipeline
    ```

### Automated Setup

The `script.sh` file automates the setup and execution of the pipeline. It installs necessary dependencies and runs the pipeline.

Run the automated setup script:

	```bash
	./script.sh
	```

### Manual Setup

If you prefer manual setup, follow these steps:

1. **Install the required Python packages**:

	```bash
	pip install -r requirements.txt
	```

2. **Run the pipeline script**:

	```bash
	python pipeline.py
	```

## Usage

To run the pipeline and perform the following tasks, use the `script.sh`:

- Run unit tests.
- Process data.
- Update the database.


## Folder Structure

### Root Directory
- **README.md**: this file
- **script.sh**: Bash script for setup and execution. 
- **requirements.txt**: Python dependencies.
- **cademycode.db**: Initial database with tables:
    - `cademycode_students`
    - `cademycode_student_jobs`
    - `cademycode_courses`
- **cademycode_updated.db**: Updated database for testing.

### Generated Files
After the first run, the following files are created:

- **version.txt**: Contains and updates the current version.
- **row_counts.json**: Records current row counts of tables.
- **changelog.log**: Logs information from process steps.
- **errorlog.log**: Logs errors from process steps.
- **merged_data.csv**: Merged dataframes output from `pipeline.py`.

### 'dev' Directory
- pipeline.py: Data processing script.
- initial_exploration.ipynb: Initial data exploration notebook.
- merged_data_initial.csv: Initial merged dataframes.

### 'build' Directory
- pipeline.py: Copied from `dev` directory.
- merged_data.csv: Generated merged dataframes.

### 'tests' Directory
- tests.py: Unit tests for `pipeline.py`
- testlog.log: Logs information from unit tests.


## Testing

Unit tests are defined in `tests.py` and run before executing `pipeline.py`. They validate data transformations, handle error logging, and ensure the integrity of the pipeline operations.