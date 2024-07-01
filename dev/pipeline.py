import sqlalchemy
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Integer, String, DateTime, Float, text
import json
import logging
import pandas as pd
import os

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
DB_URL = "sqlite:///cademycode.db"

# create a database connection
def create_db_connection(db_url):
    return create_engine(db_url)


# Function to get row counts for all tables
def get_row_counts(engine):
    with engine.connect() as connection:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
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


# Function to update the database
def update_database(engine):
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    dataframes = {}
    if not tables:
        logging.warning("No tables found in the database.")
    else:
        logging.info("Tables in the database:")
        for table in tables:
            logging.info(table)
            df = pd.read_sql_table(table, engine)
            dataframes[table] = df
            logging.info(f"Data from {table}:")
            logging.info(df.head())
    return dataframes


# Function to process DataFrames
def process_dataframes(dataframes):
    df_students = dataframes.get('cademycode_students', pd.DataFrame())
    df_jobs = dataframes.get('cademycode_student_jobs', pd.DataFrame())
    df_courses = dataframes.get('cademycode_courses', pd.DataFrame())

    # Clean and process df_students
    df_students['job_id'] = pd.to_numeric(df_students['job_id'], errors='coerce')
    df_students['num_course_taken'] = pd.to_numeric(df_students['num_course_taken'], errors='coerce')
    df_students['time_spent_hrs'] = pd.to_numeric(df_students['time_spent_hrs'], errors='coerce')
    df_students['current_career_path_id'] = pd.to_numeric(df_students['current_career_path_id'], errors='coerce')
    
    df_students_clean = df_students.dropna(subset=['job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs'])

    df_students_clean.loc[:, 'job_id'] = df_students_clean['job_id'].astype(int)
    df_students_clean.loc[:, 'num_course_taken'] = df_students_clean['num_course_taken'].astype(int)
    df_students_clean.loc[:, 'time_spent_hrs'] = df_students_clean['time_spent_hrs'].astype(float)
    df_students_clean.loc[:, 'contact_info'] = df_students_clean['contact_info'].apply(json.dumps)
    df_students_clean.loc[:, 'current_career_path_id'] = df_students_clean['current_career_path_id'].astype(int)
    
    # Drop duplicates from df_jobs
    df_jobs_clean = df_jobs.drop_duplicates()
    
    logging.info("df_students NaN values and df_jobs duplicates dropped")

    return df_students_clean, df_jobs_clean, df_courses


# Function to save DataFrames back to the database
def update_db_table(engine, df_students_clean):
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
    df_students_clean.to_sql('cademycode_students2', engine, if_exists='replace', index=False)
  
    # Drop old table and rename new table
    with engine.connect() as conn:
        drop_statement = text("DROP TABLE IF EXISTS cademycode_students")
        conn.execute(drop_statement)
        rename_statement = text("ALTER TABLE cademycode_students2 RENAME TO cademycode_students")
        conn.execute(rename_statement)
    changelog.info('Database updated successfully.')


# Merge DataFrames
def merge_df(df_students, df_jobs_clean, df_courses):
    merged_df = pd.merge(df_students, df_jobs_clean, on='job_id', how='inner')
    changelog.info('df_students and df_jobs_clean merged')

    merged_df = pd.merge(merged_df, df_courses, left_on='current_career_path_id', right_on='career_path_id', how='inner')
    changelog.info('df_courses merged to df_students_clean')

    return merged_df


# Main function
def main():
    engine = create_db_connection(DB_URL)
    last_row_counts = {
        "cademycode_courses": 1,
        "cademycode_student_jobs": 13,
        "cademycode_students": 5000
    }
    try:
        if not check_for_updates(engine, last_row_counts):
            logging.info("No updates found in the database.")
        else:
            logging.info("Updates found in the database.")
            dataframes = update_database(engine)
            df_students_clean, df_jobs_clean, df_courses = process_dataframes(dataframes)
            update_db_table(engine, df_students_clean)
            merged_df = merge_df(df_students_clean, df_jobs_clean, df_courses)
            merged_df.to_csv('merged_data.csv', index=False)
            logging.info("Data processing and merging completed successfully.")
    except Exception as e:
        errorlog.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()