import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column, Integer, String, Float
import json
import logging
import os
import unittest
import sys


def configure_logging(log_file_prefix='../', app_version='1.0.0', test_env=False):
	# Configure logging
	logging.basicConfig(level=logging.INFO, format=f'%(asctime)s - %(levelname)s - {app_version} - %(message)s')
	changelog = logging.getLogger('changelog')
	errorlog = logging.getLogger('errorlog')

	# Create a formatter with timestamp
	formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {app_version} - %(message)s')

	if not test_env:
		# Create file handlers and set formatter
		changelog_handler = logging.FileHandler(os.path.join(log_file_prefix, 'changelog.log'))
		changelog_handler.setFormatter(formatter)
		errorlog_handler = logging.FileHandler(os.path.join(log_file_prefix, 'errorlog.log'))
		errorlog_handler.setFormatter(formatter)

		# Add handlers to the loggers
		changelog.addHandler(changelog_handler)
		errorlog.addHandler(errorlog_handler)

	return changelog, errorlog


# Database connection details
db_url = "sqlite:///cademycode.db"
row_counts_file = 'row_counts.json'

def update_version(version_file='version.txt'):
	# Read the current version
	if os.path.exists(version_file):
		with open(version_file, 'r') as file:
			version = file.read().strip()
	else:
		version = "1.0.0"

	# Split the version into major, minor, and patch
	major, minor, patch = map(int, version.split('.'))

	# Increment the patch number
	patch += 1

	# Update the version number
	new_version = f"{major}.{minor}.{patch}"

	# Write the new version to the file
	with open(version_file, 'w') as file:
		file.write(new_version)

	return new_version


def log_version_change(changelog, version_file='version.txt'):
	# Update the version
	new_version = update_version(version_file)

	# Log the version change
	changelog.info(f"Version {new_version}: Updates applied.")
	return new_version


# Create a database engine
def create_db_engine(db_url):
	return create_engine(db_url)


def run_tests():
	# Run the unittests in the tests directory
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


# Function to initialize row counts to 1 for all tables
def initialize_row_counts(engine, file_path):
	table_names = get_table_names(engine)
	row_counts = {table: 1 for table in table_names}
	with open(file_path, 'w') as file:
		json.dump(row_counts, file)
	changelog.info(f"Initialized row counts: {row_counts}")
	return row_counts


# Function to check for database updates based on row counts
def check_for_updates(engine, last_row_counts, changelog, current_row_counts=None):
	if current_row_counts is None:
		current_row_counts = get_row_counts(engine)

	changelog.info("Checking for updates...")
	changelog.info(f"Last row counts: {last_row_counts}")
	changelog.info(f"Current row counts: {current_row_counts}")

	for table, count in current_row_counts.items():
		if table not in last_row_counts or last_row_counts[table] != count:
			changelog.info(f"Update found in table '{table}': last count = {last_row_counts.get(table)}, current count = {count}")
			return True
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
def get_dataframes(engine, changelog):
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
def process_dataframes(dataframes, changelog):
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


def merge_df(df_students, df_jobs, df_courses, changelog):
	merged_df = df_students.merge(df_jobs, on='job_id', how='inner')
	changelog.info('df_students and df_jobs merged')

	merged_df = merged_df.merge(df_courses, left_on='current_career_path_id', right_on='career_path_id', how='inner')
	changelog.info('df_courses merged to merged_df')
	return merged_df


def main():
	global changelog, errorlog

	version_file = 'version.txt'
	new_version = update_version(version_file)
	changelog, errorlog = configure_logging(app_version=new_version)

	engine = create_db_engine(db_url)

	# Get row counts
	if not os.path.exists(row_counts_file) or os.path.getsize(row_counts_file) == 0:
		last_row_counts = initialize_row_counts(engine, row_counts_file)
	else:
		last_row_counts = load_row_counts(row_counts_file)
	current_row_counts = get_row_counts(engine)

	try:
		if check_for_updates(engine, last_row_counts, changelog, current_row_counts):
			changelog.info("Updates found. Processing data...")

			# Get DataFrames
			dataframes = get_dataframes(engine, changelog)

			# Process DataFrames
			df_students, df_jobs, df_courses = process_dataframes(dataframes, changelog)

			# Save DataFrames back to the database
			update_db_tables(engine, df_students, df_jobs, df_courses)

			# Save the current row counts
			current_row_counts = get_row_counts(engine)
			save_row_counts(current_row_counts, row_counts_file)

			# Merge dataframes into CSV
			merged_df = merge_df(df_students, df_jobs, df_courses, changelog)
			merged_df.to_csv('merged_data.csv', index=False)

			changelog.info("Data processing and merging completed successfully.")

		else:
			changelog.info("No updates found.")
	except Exception as e:
		errorlog.error(f"An error occurred: {e}")
		raise

 
if __name__ == '__main__':
	if 'test' in sys.argv:
		test_log_prefix = '../tests'
		changelog, errorlog = configure_logging(log_file_prefix=test_log_prefix, app_version='test_version', test_env=True)

		result = run_tests()
		sys.exit(0 if result else 1)
	else:
		main()
