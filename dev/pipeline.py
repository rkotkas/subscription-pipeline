import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column, Integer, String, Float
import json
import logging
import os
import unittest
import sys


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
changelog = logging.getLogger('changelog')
errorlog = logging.getLogger('errorlog')

# Create a formatter with timestamp
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create file handlers and set formatter
changelog_handler = logging.FileHandler('../changelog.log')

changelog_handler.setFormatter(formatter)
errorlog_handler = logging.FileHandler('../errorlog.log')
errorlog_handler.setFormatter(formatter)

# Add handlers to the loggers
changelog.addHandler(changelog_handler)
errorlog.addHandler(errorlog_handler)


# Database connection details
db_url = "sqlite:///cademycode.db"
row_counts_file = 'row_counts.json'

# Create a database engine
def create_db_engine(db_url):
	return create_engine(db_url)


def run_tests():
    """Run the unittests in the tests directory."""
    test_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tests')
    loader = unittest.TestLoader()
    tests = loader.discover(start_dir=test_dir)
    testRunner = unittest.TextTestRunner()
    result = testRunner.run(tests)
    return result.wasSuccessful()


# get table names from database
def get_table_names(engine):
	inspector = inspect(engine)
	table_names = inspector.get_table_names()
	return table_names


def get_row_counts(engine):
	with engine.connect() as connection:
		inspector = inspect(engine)
		tables = inspector.get_table_names()
		row_counts = {}
		for table in tables:
			try:
				result = connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
				row_counts[table] = result
				changelog.info(f"Row count for table '{table}': {result}")
			except Exception as e:
				changelog.error(f"Error retrieving row count for table '{table}': {str(e)}")
		return row_counts


# Function to check for database updates based on row counts
def check_for_updates(engine, last_row_counts, current_row_counts=None):
	if current_row_counts is None:
		current_row_counts = get_row_counts(engine)

	changelog.info("Checking for updates...")
	changelog.info(f"Last row counts: {last_row_counts}")
	changelog.info(f"Current row counts: {current_row_counts}")

	for table, count in current_row_counts.items():
		if table not in last_row_counts or last_row_counts[table] != count:
			changelog.info(f"Update found in table '{table}': last count = {last_row_counts.get(table)}, current count = {count}")
			return True
	changelog.info("No updates found.")
	return False


# Function to save row counts to a JSON file
def save_row_counts(row_counts, file_path):
    with open(file_path, 'w') as file:
        json.dump(row_counts, file)


# Function to load row counts from a JSON file
def load_row_counts(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    else:
        return {}


# Function to get dataframes for all tables
def get_dataframes(engine):
	table_names = get_table_names(engine)
	dataframes = {}
	if not table_names:
		changelog.warning("No tables found in the database.")
	else:
		changelog.info("Tables in the database:")
		for table_name in table_names:
			changelog.info(table_name)
			df = pd.read_sql_table(table_name, engine)
			dataframes[table_name] = df
			changelog.info(f"Data from {table_name}:")
			changelog.info(df.head().to_string())  # Convert DataFrame head to string
	return dataframes


# remove NaN and duplictes
def process_dataframes(dataframes):
	# Clean and process df_students
	df_students = dataframes.get('cademycode_students', pd.DataFrame())
	df_jobs = dataframes.get('cademycode_student_jobs', pd.DataFrame())
	df_courses = dataframes.get('cademycode_courses', pd.DataFrame())

	df_students['job_id'] = pd.to_numeric(df_students['job_id'], errors='coerce')
	df_students['num_course_taken'] = pd.to_numeric(df_students['num_course_taken'], errors='coerce')
	df_students['time_spent_hrs'] = pd.to_numeric(df_students['time_spent_hrs'], errors='coerce')
	df_students['current_career_path_id'] = pd.to_numeric(df_students['current_career_path_id'], errors='coerce')

	df_students_clean = df_students.copy()
	nan_counts = df_students_clean.isna().sum()

	# Drop rows with NaN values
	if (nan_counts != 0).any():
		changelog.info(f"Dropping df_students NaN values {nan_counts}")
		df_students_clean = df_students.dropna(subset=['job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs']).copy()
		changelog.info("NaN values dropped")
	else:
		changelog.info("No NaN values found in df_students")

	df_students_clean['job_id'] = df_students_clean['job_id'].astype(int)
	df_students_clean['num_course_taken'] = df_students_clean['num_course_taken'].astype(int)
	df_students_clean['time_spent_hrs'] = df_students_clean['time_spent_hrs'].astype(float)
	df_students_clean['contact_info'] = df_students_clean['contact_info'].apply(json.dumps)
	df_students_clean['current_career_path_id'] = df_students_clean['current_career_path_id'].astype(int)

	# Drop duplicated values
	df_jobs_clean = df_jobs.copy()
	jobs_duplicates = df_jobs.duplicated()
	if jobs_duplicates.any():
		changelog.info(f"Dropping df_jobs duplicates {df_jobs.duplicated()}")
		df_jobs_clean = df_jobs.drop_duplicates()
		changelog.info("Duplicates dropped")
	else:
		changelog.info("No duplicates found in df_jobs")

	changelog.info("Dataframes processing completed")

	return df_students_clean, df_jobs_clean, df_courses


# Function to save DataFrames back to the database
def update_db_tables(engine, df_students, df_jobs, df_courses):
	metadata = MetaData()

	cademycode_students2 = Table(
		'cademycode_students2',
		metadata,
		Column('uuid', Integer, primary_key=True),
		Column('name', String),
		Column('dob', String),
		Column('sex', String),
		Column('contact_info', String),
		Column('job_id', Integer),
		Column('num_course_taken', Integer),
		Column('current_career_path_id', Integer),
		Column('time_spent_hrs', Float)
	)

	cademycode_jobs2 = Table(
		'cademycode_jobs2',
		metadata,
		Column('job_id', Integer, primary_key=True),
		Column('job_category', String),
		Column('avg_salary', Integer)
	)

	cademycode_courses2 = Table(
		'cademycode_courses2',
		metadata,
		Column('career_path_id', Integer, primary_key=True),
		Column('career_path_name', String),
		Column('hours_to_complete', Integer)
	)

	with engine.begin() as conn:
		try:
			# Create new tables
			metadata.create_all(conn)

			# Insert data into new table
			df_students.to_sql('cademycode_students2', engine, if_exists='replace', index=False)
			df_jobs.to_sql('cademycode_jobs2', engine, if_exists='replace', index=False)
			df_courses.to_sql('cademycode_courses2', engine, if_exists='replace', index=False)

			# Drop old table and rename new table
			drop_cademycode_students = text("DROP TABLE IF EXISTS cademycode_students")
			drop_cademycode_jobs = text("DROP TABLE IF EXISTS cademycode_student_jobs")
			drop_cademycode_courses = text("DROP TABLE IF EXISTS cademycode_courses")

			conn.execute(drop_cademycode_students)
			conn.execute(drop_cademycode_jobs)
			conn.execute(drop_cademycode_courses)

			rename_drop_cademycode_students = text("ALTER TABLE cademycode_students2 RENAME TO cademycode_students")
			rename_drop_cademycode_jobs = text("ALTER TABLE cademycode_jobs2 RENAME TO cademycode_student_jobs")
			rename_drop_cademycode_courses = text("ALTER TABLE cademycode_courses2 RENAME TO cademycode_courses")

			conn.execute(rename_drop_cademycode_students)
			conn.execute(rename_drop_cademycode_jobs)
			conn.execute(rename_drop_cademycode_courses)

			changelog.info('Database updated successfully.')

		except Exception as e:
			changelog.error(f"Error updating database: {e}")
			raise


def merge_df(df_students, df_jobs, df_courses):
	merged_df = df_students.merge(df_jobs, on='job_id', how='inner')
	changelog.info('df_students and df_jobs merged')

	merged_df = merged_df.merge(df_courses, left_on='current_career_path_id', right_on='career_path_id', how='inner')
	changelog.info('df_courses merged to merged_df')
	return merged_df

def main():
	# Run tests before pipeline execution
	if not run_tests():
		sys.exit("Tests failed. Aborting pipeline execution.")

	# Pipeline logic
	changelog.info("Running pipeline...")

	engine = create_db_engine(db_url)
	last_row_counts = load_row_counts(row_counts_file)
	try:
		if not check_for_updates(engine, last_row_counts):
			changelog.info("No updates found in the database.")
		else:
			changelog.info("Updates found in the database.")
			dataframes = get_dataframes(engine)
			df_students, df_jobs, df_courses = process_dataframes(dataframes)
			update_db_tables(engine, df_students, df_jobs, df_courses)

			current_row_counts = get_row_counts(engine)
			save_row_counts(current_row_counts, row_counts_file)
			changelog.info(f"Row count after changes: {current_row_counts}")

			merged_df = merge_df(df_students, df_jobs, df_courses)
			merged_df.to_csv('merged_data.csv', index=False)
			changelog.info("Data processing and merging completed successfully.")
	except Exception as e:
		errorlog.error(f"An error occurred: {e}")
		raise

if __name__ == "__main__":
    main()