# Project Writeup: Building a Data Engineering Pipeline for a Subscription-Based Company

## Overview
In this project, I developed a data engineering pipeline designed to regularly transform a messy and inconsistent database into a clean and reliable source of truth for an analytics team. The pipeline was designed with automation in mind to minimize human intervention, ensuring that the data is always ready for analysis with minimal manual effort.

## Scenario
The project simulates a real-world scenario where a fictional subscription-based company, Cademycode, manages a database of long-term cancelled subscribers. This database is frequently updated from multiple disparate sources, leading to inconsistencies and data quality issues. The goal of the pipeline is to clean and transform this data into a format that can be easily used by the analytics team to derive insights, ensuring that the data remains accurate and consistent over time.

## About the Data
Given the importance of data privacy, a realistic but entirely fictional dataset was used for this project. The dataset represents customer information for Cademycode, a fictional online education company. The dataset includes various fields such as customer IDs, subscription statuses, demographic information, and engagement metrics.

The database was intentionally designed to be messy, including common issues such as missing values, incorrect data types, inconsistent formatting, and duplicate records. These issues provided a realistic environment for practicing data cleaning and transformation skills.

## Project Objectives
The primary objectives of this project were to:
- Develop a data engineering project to showcase in a portfolio.
- Use Jupyter notebooks to explore and clean the dataset.
- Employ Python to automate the data cleaning and transformation process, including the use of unit tests and logging to ensure robustness.
- Utilize Bash scripts to automate file management and execute the data transformation pipeline.

## Technical Approach
### 1. Data Exploration and Cleaning Using Jupyter Notebooks:
- The first step involved loading the dataset into a Jupyter notebook and performing an initial exploration to understand its structure and identify common data quality issues.
- Using Pandas, I conducted various data cleaning operations, including:
 	* Identifying and removing duplicate records to maintain the integrity of the dataset.
	* Handling missing values by removing incomplete records.
	* Converting data types, such as transforming date strings into datetime64 objects, converting job ID, number of courses taken, and current career path ID into integers, converting time spent in hours into floats, and serializing contact information as JSON strings.

### 2. Automating Data Transformation with Python:
- After manual exploration, I developed two key Python scripts, `pipeline.py` and `tests.py`, to automate and validate the data cleaning and transformation process. These scripts include:
	* **Automated Data Transformation**: The `pipeline.py` script orchestrates the data processing workflow, handling tasks such as type conversions, deduplication, updating database, and table merging.

	* **Unit Testing**: The `tests.py` script includes a robust suite of unit tests to verify the correctness of the pipeline. These tests focus on various aspects of the data processing functions and include:

		- Database Connection Verification: The test_create_db_engine test confirms that a database engine is successfully created and can execute basic queries, ensuring that the database connection is reliable.

		- Update Check Logic: The test_check_for_updates_no_change, test_check_for_updates_with_change, and test_check_for_updates_new_table tests simulate scenarios where the number of rows in tables may or may not change. These tests ensure that the pipeline accurately detects when updates are required, preventing unnecessary processing.

		- Table Name Retrieval: The test_get_table_names test ensures that all expected tables are present in the database and can be retrieved correctly.

		- DataFrame Integrity: Several tests, including test_get_dataframes, test_df_students_not_empty, test_df_jobs_not_empty, and test_merged_df_row_count, verify that data is correctly loaded into Pandas DataFrames and that the DataFrames are not empty after loading.

		- DataFrame Cleaning and Processing: The test_df_students_nan_dropped and test_df_jobs_duplicated_dropped tests ensure that NaN values and duplicate rows are properly removed during data cleaning, maintaining the integrity of the datasets.

		- Data Type Validation: The test_df_students_dtypes test confirms that the data types of key columns are correctly converted, including checks for datetime64[ns] for date columns, integers for ID columns, and floats for numerical values such as time spent.

		- Merged DataFrame Structure: The test_merged_df_columns test verifies that the final merged DataFrame contains all expected columns, ensuring that the merge operation between the students, jobs, and courses data is performed correctly.

	    By implementing these unit tests, the project ensures that each component of the data pipeline works as expected, providing confidence in the accuracy and efficiency of the overall system.

	* **Logging Mechanism**: Three separate log files are utilized:
		- Change Log: Captures details of any changes made to the data during processing.
		- Error Log: Records any errors or anomalies encountered during the pipeline's execution, enabling swift identification and resolution of issues.
		- Test Log: Tracks the execution of unit tests, providing insights into test outcomes and any potential failures.
	
        This setup ensures that as new data is ingested, it is consistently processed and validated, maintaining a reliable source of clean data for analysis.

- The pipeline can be rerunned when the database receives new updates, ensuring that the data remains clean over time.

### 3. Automation with Bash Scripts:
- To further reduce the need for manual intervention, I wrote Bash scripts to automate various aspects of the pipeline, including:
	* Environment Setup: Installing necessary dependencies using a requirements.txt file.
	* Data Management: Executing the Python data transformation script, and organizing the output files for the analytics team.


## Conclusion
This project successfully demonstrated the development of a semi-automated data engineering pipeline capable of transforming messy and inconsistent data into a clean and reliable dataset for analytics purposes. The combination of Python, Jupyter notebooks, and Bash scripting provided a powerful toolkit for automating the process, from initial data cleaning to final output. The pipeline not only improves the efficiency of data management but also ensures that the analytics team has access to accurate and consistent data, enabling better decision-making for the fictional Cademycode company.

This project showcases the practical application of data engineering principles and tools, making it a valuable addition to my portfolio as a demonstration of my ability to handle real-world data challenges in a professional setting.