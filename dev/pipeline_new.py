import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column, Integer, String, Float
import json


# Database connection details
db_url = "sqlite:///cademycode.db"

# Create a database engine
def create_db_engine(db_url):
	return create_engine(db_url)


# get table names from database
def get_table_names(engine):
	inspector = inspect(engine)
	table_names = inspector.get_table_names()
	return table_names


# Function to get row counts for all tables
def get_row_counts(engine):
	with engine.connect() as connection:
		tables = get_table_names(engine)
		row_counts = {}
		for table in tables:
			result = connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
			row_counts[table] = result
		return row_counts


# Function to check for database updates based on row counts
def check_for_updates(engine, last_row_counts):
	current_row_counts = get_row_counts(engine)
	for table, count in current_row_counts.items():
		if table not in last_row_counts or last_row_counts[table] != count:
			return True
	return False


# Function to get dataframes for all tables
def get_dataframes(engine):
	table_names = get_table_names(engine)
	dataframes = {}
	for table_name in table_names:
		df = pd.read_sql_table(table_name, engine)
		dataframes[table_name] = df
	#print("dataframes : ", dataframes)
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

	# Drop rows with NaN values in specified columns
	df_students_clean = df_students.dropna(subset=['job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs']).copy()

	df_students_clean['job_id'] = df_students_clean['job_id'].astype(int)
	df_students_clean['num_course_taken'] = df_students_clean['num_course_taken'].astype(int)
	df_students_clean['time_spent_hrs'] = df_students_clean['time_spent_hrs'].astype(float)
	df_students_clean['contact_info'] = df_students_clean['contact_info'].apply(json.dumps)
	df_students_clean['current_career_path_id'] = df_students_clean['current_career_path_id'].astype(int)
    
	df_jobs = dataframes.get('cademycode_student_jobs', pd.DataFrame())
	df_jobs_clean = df_jobs.drop_duplicates()

	return df_students_clean, df_jobs_clean, df_courses


def process_df_jobs(dataframes):
	df_jobs = dataframes.get('cademycode_student_jobs', pd.DataFrame())
	df_jobs_clean = df_jobs.drop_duplicates()
	# changelog.info("df_jobs duplicates dropped")
	return df_jobs_clean


# Function to save DataFrames back to the database
def update_db_table(engine, df_students):
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

    # Create new table
    metadata.create_all(engine)

    # Insert data into new table
    df_students.to_sql('cademycode_students2', engine, if_exists='replace', index=False)
  
    # Drop old table and rename new table
    with engine.connect() as conn:
        drop_statement = text("DROP TABLE IF EXISTS cademycode_students")
        conn.execute(drop_statement)
        rename_statement = text("ALTER TABLE cademycode_students2 RENAME TO cademycode_students")
        conn.execute(rename_statement)
   # changelog.info('Database updated successfully.')


def merge_df(df_students, df_jobs, df_courses):
	merged_df = df_students.merge(df_jobs, on='job_id', how='inner')
	#changelog.info('df_students and df_jobs merged')

	merged_df = merged_df.merge(df_courses, left_on='current_career_path_id', right_on='career_path_id', how='inner')
	#changelog.info('df_courses merged to df_students')
	return merged_df


if __name__ == "__main__":
	engine = create_db_engine(db_url)
	last_row_counts = {
		"cademycode_courses": 1,
		"cademycode_student_jobs": 13,
		"cademycode_students": 5000
	}
	try:
		if not check_for_updates(engine, last_row_counts):
			print("No updates found in the database.")
		else:
			dataframes = get_dataframes(engine)
			df_students, df_jobs, df_courses = process_dataframes(dataframes)
			update_db_table(engine, df_students)
			merged_df = merge_df(df_students, df_jobs, df_courses)
			merged_df.to_csv('merged_data.csv', index=False)
	except Exception as e:
		print(f"An error occurred: {e}")
		raise