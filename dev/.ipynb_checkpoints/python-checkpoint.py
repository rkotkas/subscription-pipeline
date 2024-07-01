{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "c97ac4ed-1fe9-420b-b88c-950be1dda121",
   "metadata": {},
   "outputs": [],
   "source": [
    "from sqlalchemy import create_engine, inspect\n",
    "import pandas as pd\n",
    "\n",
    "# Path to the database file\n",
    "db_path = 'cademycode.db'\n",
    "\n",
    "# Create a database engine\n",
    "engine = create_engine(f'sqlite:///{db_path}')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "62b65ef9-1d46-4694-9661-18092b359d68",
   "metadata": {},
   "outputs": [],
   "source": [
    "# inspector to get the list of tables\n",
    "inspector = inspect(engine)\n",
    "tables = inspector.get_table_names()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "71e96bb2-e521-4822-80f4-2520d4163a9f",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Tables in the database:\n",
      "cademycode_courses\n",
      "Data from cademycode_courses:\n",
      "   career_path_id      career_path_name  hours_to_complete\n",
      "0               1        data scientist                 20\n",
      "1               2         data engineer                 20\n",
      "2               3          data analyst                 12\n",
      "3               4  software engineering                 25\n",
      "4               5      backend engineer                 18\n",
      "cademycode_student_jobs\n",
      "Data from cademycode_student_jobs:\n",
      "   job_id        job_category  avg_salary\n",
      "0       1           analytics       86000\n",
      "1       2            engineer      101000\n",
      "2       3  software developer      110000\n",
      "3       4            creative       66000\n",
      "4       5  financial services      135000\n",
      "cademycode_students\n",
      "Data from cademycode_students:\n",
      "   uuid             name         dob sex  \\\n",
      "0     1  Annabelle Avery  1943-07-03   F   \n",
      "1     2      Micah Rubio  1991-02-07   M   \n",
      "2     3       Hosea Dale  1989-12-07   M   \n",
      "3     4     Mariann Kirk  1988-07-31   F   \n",
      "4     5  Lucio Alexander  1963-08-31   M   \n",
      "\n",
      "                                        contact_info  job_id  \\\n",
      "0  {\"mailing_address\": \"303 N Timber Key, Irondal...       7   \n",
      "1  {\"mailing_address\": \"767 Crescent Fair, Shoals...       7   \n",
      "2  {\"mailing_address\": \"P.O. Box 41269, St. Bonav...       7   \n",
      "3  {\"mailing_address\": \"517 SE Wintergreen Isle, ...       6   \n",
      "4  {\"mailing_address\": \"18 Cinder Cliff, Doyles b...       7   \n",
      "\n",
      "   num_course_taken  career_path_id  time_spent_hrs  \n",
      "0                 6               1            4.99  \n",
      "1                 5               8            4.40  \n",
      "2                 8               8            6.74  \n",
      "3                 7               9           12.31  \n",
      "4                14               3            5.64  \n"
     ]
    }
   ],
   "source": [
    "# Import each table into a DataFrame\n",
    "dataframes = {}\n",
    "if not tables:\n",
    "    print(\"No tables found in the database.\")\n",
    "else:\n",
    "    print(\"Tables in the database:\")\n",
    "    for table in tables:\n",
    "        print(table)\n",
    "        df = pd.read_sql_table(table, engine)\n",
    "        dataframes[table] = df\n",
    "        print(f\"Data from {table}:\")\n",
    "        print(df.head())\n",
    "\n",
    "# Now all the tables have been loaded into DataFrames stored in the `dataframes` dictionary"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "14d698da-fece-4709-8e83-80301135444d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "   career_path_id      career_path_name  hours_to_complete\n",
      "0               1        data scientist                 20\n",
      "1               2         data engineer                 20\n",
      "2               3          data analyst                 12\n",
      "3               4  software engineering                 25\n",
      "4               5      backend engineer                 18\n"
     ]
    }
   ],
   "source": [
    "# Access a DataFrame by index\n",
    "table_name = tables[0]\n",
    "df_courses = dataframes[table_name]\n",
    "\n",
    "# Display the first few rows\n",
    "print(df_courses.head())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "id": "21adebe8-4db2-41eb-90db-9f518dce5902",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "   job_id        job_category  avg_salary\n",
      "0       1           analytics       86000\n",
      "1       2            engineer      101000\n",
      "2       3  software developer      110000\n",
      "3       4            creative       66000\n",
      "4       5  financial services      135000\n"
     ]
    }
   ],
   "source": [
    "table_name = tables[1]\n",
    "df_jobs = dataframes[table_name]\n",
    "\n",
    "# Display the first few rows\n",
    "print(df_jobs.head())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "6ec95689-2a10-4d92-a836-5b80f49fdb3b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "   uuid             name         dob sex  \\\n",
      "0     1  Annabelle Avery  1943-07-03   F   \n",
      "1     2      Micah Rubio  1991-02-07   M   \n",
      "2     3       Hosea Dale  1989-12-07   M   \n",
      "3     4     Mariann Kirk  1988-07-31   F   \n",
      "4     5  Lucio Alexander  1963-08-31   M   \n",
      "\n",
      "                                        contact_info job_id num_course_taken  \\\n",
      "0  {'mailing_address': '303 N Timber Key, Irondal...    7.0              6.0   \n",
      "1  {'mailing_address': '767 Crescent Fair, Shoals...    7.0              5.0   \n",
      "2  {'mailing_address': 'P.O. Box 41269, St. Bonav...    7.0              8.0   \n",
      "3  {'mailing_address': '517 SE Wintergreen Isle, ...    6.0              7.0   \n",
      "4  {'mailing_address': '18 Cinder Cliff, Doyles b...    7.0             14.0   \n",
      "\n",
      "  current_career_path_id time_spent_hrs  \n",
      "0                    1.0           4.99  \n",
      "1                    8.0            4.4  \n",
      "2                    8.0           6.74  \n",
      "3                    9.0          12.31  \n",
      "4                    3.0           5.64  \n"
     ]
    }
   ],
   "source": [
    "table_name = tables[2]\n",
    "df_students = dataframes[table_name]\n",
    "\n",
    "# Display the first few rows\n",
    "print(df_students.head())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "id": "7ed19c5b-0ef3-44eb-a23a-a7360695503d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 10 entries, 0 to 9\n",
      "Data columns (total 3 columns):\n",
      " #   Column             Non-Null Count  Dtype \n",
      "---  ------             --------------  ----- \n",
      " 0   career_path_id     10 non-null     int64 \n",
      " 1   career_path_name   10 non-null     object\n",
      " 2   hours_to_complete  10 non-null     int64 \n",
      "dtypes: int64(2), object(1)\n",
      "memory usage: 372.0+ bytes\n",
      "None\n"
     ]
    }
   ],
   "source": [
    "print(df_courses.info())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "b9a920e6-ce8e-495a-8018-88803a6e871c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "       career_path_id  hours_to_complete\n",
      "count        10.00000          10.000000\n",
      "mean          5.50000          21.900000\n",
      "std           3.02765           6.707376\n",
      "min           1.00000          12.000000\n",
      "25%           3.25000          18.500000\n",
      "50%           5.50000          20.000000\n",
      "75%           7.75000          26.500000\n",
      "max          10.00000          35.000000\n"
     ]
    }
   ],
   "source": [
    "print(df_courses.describe())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "id": "0644dd4a-57d0-4323-82da-b1730b1480df",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 13 entries, 0 to 12\n",
      "Data columns (total 3 columns):\n",
      " #   Column        Non-Null Count  Dtype \n",
      "---  ------        --------------  ----- \n",
      " 0   job_id        13 non-null     int64 \n",
      " 1   job_category  13 non-null     object\n",
      " 2   avg_salary    13 non-null     int64 \n",
      "dtypes: int64(2), object(1)\n",
      "memory usage: 444.0+ bytes\n",
      "None\n"
     ]
    }
   ],
   "source": [
    "print(df_jobs.info())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "1b98e4d4-c32a-48a8-a33a-9a8f9d89dd55",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "          job_id     avg_salary\n",
      "count  13.000000      13.000000\n",
      "mean    4.384615   89230.769231\n",
      "std     2.662657   34727.879881\n",
      "min     0.000000   10000.000000\n",
      "25%     3.000000   66000.000000\n",
      "50%     4.000000   86000.000000\n",
      "75%     6.000000  110000.000000\n",
      "max     9.000000  135000.000000\n"
     ]
    }
   ],
   "source": [
    "print(df_jobs.describe())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "id": "f449df58-9fe0-4c92-a193-c6e8ac6d5954",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 5000 entries, 0 to 4999\n",
      "Data columns (total 9 columns):\n",
      " #   Column                  Non-Null Count  Dtype \n",
      "---  ------                  --------------  ----- \n",
      " 0   uuid                    5000 non-null   int64 \n",
      " 1   name                    5000 non-null   object\n",
      " 2   dob                     5000 non-null   object\n",
      " 3   sex                     5000 non-null   object\n",
      " 4   contact_info            5000 non-null   object\n",
      " 5   job_id                  4995 non-null   object\n",
      " 6   num_course_taken        4749 non-null   object\n",
      " 7   current_career_path_id  4529 non-null   object\n",
      " 8   time_spent_hrs          4529 non-null   object\n",
      "dtypes: int64(1), object(8)\n",
      "memory usage: 351.7+ KB\n",
      "None\n"
     ]
    }
   ],
   "source": [
    "print(df_students.info())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 38,
   "id": "583ec262-74f4-48ea-8b05-2cd1b0e3d5fa",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "              uuid\n",
      "count  5000.000000\n",
      "mean   2500.500000\n",
      "std    1443.520003\n",
      "min       1.000000\n",
      "25%    1250.750000\n",
      "50%    2500.500000\n",
      "75%    3750.250000\n",
      "max    5000.000000\n"
     ]
    }
   ],
   "source": [
    "print(df_students.describe())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "id": "dff12714-d092-40d8-b280-035a14b21cb1",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "uuid                       int64\n",
      "name                      object\n",
      "dob                       object\n",
      "sex                       object\n",
      "contact_info              object\n",
      "job_id                    object\n",
      "num_course_taken          object\n",
      "current_career_path_id    object\n",
      "time_spent_hrs            object\n",
      "dtype: object\n"
     ]
    }
   ],
   "source": [
    "print(df_students.dtypes)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 56,
   "id": "267cf2d2-83d5-47c2-acef-d4ce4b7a82c6",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "   uuid             name         dob sex  \\\n",
      "0     1  Annabelle Avery  1943-07-03   F   \n",
      "1     2      Micah Rubio  1991-02-07   M   \n",
      "2     3       Hosea Dale  1989-12-07   M   \n",
      "3     4     Mariann Kirk  1988-07-31   F   \n",
      "4     5  Lucio Alexander  1963-08-31   M   \n",
      "\n",
      "                                        contact_info  job_id  \\\n",
      "0  {\"mailing_address\": \"303 N Timber Key, Irondal...       7   \n",
      "1  {\"mailing_address\": \"767 Crescent Fair, Shoals...       7   \n",
      "2  {\"mailing_address\": \"P.O. Box 41269, St. Bonav...       7   \n",
      "3  {\"mailing_address\": \"517 SE Wintergreen Isle, ...       6   \n",
      "4  {\"mailing_address\": \"18 Cinder Cliff, Doyles b...       7   \n",
      "\n",
      "   num_course_taken  current_career_path_id  time_spent_hrs  \n",
      "0                 6                       1            4.99  \n",
      "1                 5                       8            4.40  \n",
      "2                 8                       8            6.74  \n",
      "3                 7                       9           12.31  \n",
      "4                14                       3            5.64  \n"
     ]
    }
   ],
   "source": [
    "from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, Float, text\n",
    "import json\n",
    "\n",
    "# Change 'job_id' column to float first\n",
    "df_students['job_id'] = pd.to_numeric(df_students['job_id'], errors='coerce')\n",
    "\n",
    "# Change 'num_course_taken' column to integer\n",
    "df_students['num_course_taken'] = pd.to_numeric(df_students['num_course_taken'], errors='coerce')\n",
    "\n",
    "# Change 'current_career_path_id' column to integer\n",
    "df_students['current_career_path_id'] = pd.to_numeric(df_students['current_career_path_id'], errors='coerce')\n",
    "\n",
    "# Change 'time_spent_hrs' column to integer\n",
    "df_students['time_spent_hrs'] = pd.to_numeric(df_students['time_spent_hrs'], errors='coerce')\n",
    "\n",
    "# Option 2: Drop rows with NaN values\n",
    "df_dropped = df_students.copy()\n",
    "df_dropped = df_dropped.dropna(subset=['job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs'])\n",
    "df_dropped['job_id'] = df_dropped['job_id'].astype(int)\n",
    "df_dropped['num_course_taken'] = df_dropped['num_course_taken'].astype(int)\n",
    "df_dropped['current_career_path_id'] = df_dropped['current_career_path_id'].astype(int)\n",
    "df_dropped['time_spent_hrs'] = df_dropped['time_spent_hrs'].astype(float)\n",
    "df_dropped['contact_info'] = df_dropped['contact_info'].apply(json.dumps)\n",
    "\n",
    "print(df_dropped.head())\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 71,
   "id": "dca1e1b2-896b-4e4b-b504-fdeebc868080",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Updated DataFrame in SQL:\n",
      "      uuid                      name         dob sex  \\\n",
      "0        1           Annabelle Avery  1943-07-03   F   \n",
      "1        2               Micah Rubio  1991-02-07   M   \n",
      "2        3                Hosea Dale  1989-12-07   M   \n",
      "3        4              Mariann Kirk  1988-07-31   F   \n",
      "4        5           Lucio Alexander  1963-08-31   M   \n",
      "...    ...                       ...         ...  ..   \n",
      "4288  4996          Quentin van Harn  1967-07-07   N   \n",
      "4289  4997  Alejandro van der Sluijs  1964-11-03   M   \n",
      "4290  4998            Brock Mckenzie  2004-11-25   M   \n",
      "4291  4999          Donnetta Dillard  1943-02-12   N   \n",
      "4292  5000               Elton Otway  1994-12-23   M   \n",
      "\n",
      "                                           contact_info  job_id  \\\n",
      "0     {\"mailing_address\": \"303 N Timber Key, Irondal...       7   \n",
      "1     {\"mailing_address\": \"767 Crescent Fair, Shoals...       7   \n",
      "2     {\"mailing_address\": \"P.O. Box 41269, St. Bonav...       7   \n",
      "3     {\"mailing_address\": \"517 SE Wintergreen Isle, ...       6   \n",
      "4     {\"mailing_address\": \"18 Cinder Cliff, Doyles b...       7   \n",
      "...                                                 ...     ...   \n",
      "4288  {\"mailing_address\": \"591 Blue Berry, Coulee, I...       5   \n",
      "4289  {\"mailing_address\": \"30 Iron Divide, Pewaukee ...       4   \n",
      "4290  {\"mailing_address\": \"684 Rustic Rest Avenue, C...       8   \n",
      "4291  {\"mailing_address\": \"900 Indian Oval, Euclid, ...       3   \n",
      "4292  {\"mailing_address\": \"406 Zephyr Port, Oskaloos...       2   \n",
      "\n",
      "      num_course_taken  current_career_path_id  time_spent_hrs  \n",
      "0                    6                       1            4.99  \n",
      "1                    5                       8            4.40  \n",
      "2                    8                       8            6.74  \n",
      "3                    7                       9           12.31  \n",
      "4                   14                       3            5.64  \n",
      "...                ...                     ...             ...  \n",
      "4288                 5                       2           13.82  \n",
      "4289                13                       1            7.86  \n",
      "4290                10                       3           12.10  \n",
      "4291                 6                       5           14.86  \n",
      "4292                 5                       3           10.04  \n",
      "\n",
      "[4293 rows x 9 columns]\n",
      "uuid                        int64\n",
      "name                       object\n",
      "dob                        object\n",
      "sex                        object\n",
      "contact_info               object\n",
      "job_id                      int64\n",
      "num_course_taken            int64\n",
      "current_career_path_id      int64\n",
      "time_spent_hrs            float64\n",
      "dtype: object\n"
     ]
    }
   ],
   "source": [
    "# Define new table schema\n",
    "metadata = MetaData()\n",
    "\n",
    "cademycode_students2 = Table(\n",
    "    'cademycode_students2',  # Replace with your new table name\n",
    "    metadata,\n",
    "    Column('uuid', Integer, primary_key=True),\n",
    "    Column('name', String),\n",
    "    Column('dob', String),\n",
    "    Column('sex', String),\n",
    "    Column('contact_info', String),\n",
    "    Column('job_id', Integer),\n",
    "    Column('num_course_taken', Integer),\n",
    "    Column('current_career_path_id', Integer),\n",
    "    Column('time_spent_hrs', Float)\n",
    ")\n",
    "\n",
    "# Create new table\n",
    "metadata.create_all(engine)\n",
    "\n",
    "# Insert data into new table\n",
    "df_dropped.to_sql('cademycode_students2', engine, if_exists='replace', index=False)\n",
    "\n",
    "# Drop old table and rename new table\n",
    "with engine.connect() as conn:\n",
    "    # Execute DROP TABLE statement\n",
    "    drop_statement = text(\"DROP TABLE IF EXISTS cademycode_students\")\n",
    "    conn.execute(drop_statement)\n",
    "\n",
    "    # Execute ALTER TABLE statement to rename\n",
    "    rename_statement = text(\"ALTER TABLE cademycode_students2 RENAME TO cademycode_students\")\n",
    "    conn.execute(rename_statement)\n",
    "    \n",
    "# Verify changes\n",
    "df_updated = pd.read_sql_table('cademycode_students', engine)\n",
    "print(\"Updated DataFrame in SQL:\")\n",
    "print(df_updated)\n",
    "print(df_updated.dtypes)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 89,
   "id": "06b19cbc-93de-4080-9368-c834efed10e1",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Define the SQL query to rename the column\n",
    "alter_query = text(\"ALTER TABLE cademycode_students RENAME COLUMN current_career_path_id TO career_path_id\")\n",
    "\n",
    "# Connect to the database using the engine\n",
    "with engine.connect() as conn:\n",
    "    # Execute the query using the connection\n",
    "    conn.execute(alter_query)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 131,
   "id": "0cafa60e-c5f6-4720-b877-af78ba3a30b2",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Columns in DataFrame after alteration:\n",
      "Index(['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id',\n",
      "       'num_course_taken', 'career_path_id', 'time_spent_hrs'],\n",
      "      dtype='object')\n",
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 4293 entries, 0 to 4292\n",
      "Data columns (total 9 columns):\n",
      " #   Column            Non-Null Count  Dtype  \n",
      "---  ------            --------------  -----  \n",
      " 0   uuid              4293 non-null   int64  \n",
      " 1   name              4293 non-null   object \n",
      " 2   dob               4293 non-null   object \n",
      " 3   sex               4293 non-null   object \n",
      " 4   contact_info      4293 non-null   object \n",
      " 5   job_id            4293 non-null   int64  \n",
      " 6   num_course_taken  4293 non-null   int64  \n",
      " 7   career_path_id    4293 non-null   int64  \n",
      " 8   time_spent_hrs    4293 non-null   float64\n",
      "dtypes: float64(1), int64(4), object(4)\n",
      "memory usage: 302.0+ KB\n",
      "None\n",
      "    uuid                  name         dob sex  \\\n",
      "0      1       Annabelle Avery  1943-07-03   F   \n",
      "1      2           Micah Rubio  1991-02-07   M   \n",
      "2      3            Hosea Dale  1989-12-07   M   \n",
      "3      4          Mariann Kirk  1988-07-31   F   \n",
      "4      5       Lucio Alexander  1963-08-31   M   \n",
      "5      6      Shavonda Mcmahon  1989-10-15   F   \n",
      "6      7   Terrell Bleijenberg  1959-05-05   M   \n",
      "7      8        Stanford Allan  1997-11-22   M   \n",
      "8      9       Tricia Delacruz  1961-10-20   F   \n",
      "9     10  Regenia van der Helm  1999-02-23   N   \n",
      "10    11      Shonda Stephanin  1998-10-24   F   \n",
      "11    12      Marcus Mcfarland  1977-05-29   M   \n",
      "12    13     Edwardo Boonzayer  1975-05-23   N   \n",
      "13    14        Robena Padilla  1969-01-15   F   \n",
      "14    15          Tamala Sears  1942-06-01   F   \n",
      "15    17        Maris Benskoop  1965-08-10   F   \n",
      "16    18      Yolande van Hees  1978-12-09   F   \n",
      "17    19        Dominic Werner  1973-09-29   M   \n",
      "18    21      Toney Villarreal  1993-06-04   M   \n",
      "19    22          Clayton Lamb  1963-02-02   M   \n",
      "\n",
      "                                         contact_info  job_id  \\\n",
      "0   {\"mailing_address\": \"303 N Timber Key, Irondal...       7   \n",
      "1   {\"mailing_address\": \"767 Crescent Fair, Shoals...       7   \n",
      "2   {\"mailing_address\": \"P.O. Box 41269, St. Bonav...       7   \n",
      "3   {\"mailing_address\": \"517 SE Wintergreen Isle, ...       6   \n",
      "4   {\"mailing_address\": \"18 Cinder Cliff, Doyles b...       7   \n",
      "5   {\"mailing_address\": \"P.O. Box 81591, Tarpon Sp...       6   \n",
      "6   {\"mailing_address\": \"P.O. Box 53471, Oskaloosa...       2   \n",
      "7   {\"mailing_address\": \"255 Spring Avenue, Point ...       3   \n",
      "8   {\"mailing_address\": \"997 Dewy Apple, Lake Lind...       1   \n",
      "9   {\"mailing_address\": \"220 Middle Ridge, Falcon ...       5   \n",
      "10  {\"mailing_address\": \"818 Clear Street, Rockwoo...       7   \n",
      "11  {\"mailing_address\": \"718 Embers Lane, Dale, So...       7   \n",
      "12  {\"mailing_address\": \"147 SW Plain, Solana Beac...       2   \n",
      "13  {\"mailing_address\": \"P.O. Box 73926, McLemores...       3   \n",
      "14  {\"mailing_address\": \"868 Hazy Crossing, Bethan...       7   \n",
      "15  {\"mailing_address\": \"P.O. Box 93831, South Mou...       5   \n",
      "16  {\"mailing_address\": \"460 Dusty Kennedy Cove, M...       7   \n",
      "17  {\"mailing_address\": \"P.O. Box 70430, Laramie, ...       5   \n",
      "18  {\"mailing_address\": \"P.O. Box 22286, Leisure V...       4   \n",
      "19  {\"mailing_address\": \"601 Old Oak Village, Bing...       2   \n",
      "\n",
      "    num_course_taken  career_path_id  time_spent_hrs  \n",
      "0                  6               1            4.99  \n",
      "1                  5               8            4.40  \n",
      "2                  8               8            6.74  \n",
      "3                  7               9           12.31  \n",
      "4                 14               3            5.64  \n",
      "5                 10               3           10.12  \n",
      "6                  9               8           24.17  \n",
      "7                  3               1           19.54  \n",
      "8                  6               9            1.75  \n",
      "9                  7               6           13.55  \n",
      "10                 5              10           12.61  \n",
      "11                 3               6            9.12  \n",
      "12                15               5            6.09  \n",
      "13                 5               9           15.92  \n",
      "14                13               1            4.64  \n",
      "15                 5               4           12.08  \n",
      "16                13               9           34.44  \n",
      "17                14               8           22.84  \n",
      "18                 4               3            4.30  \n",
      "19                 8               6            2.86  \n"
     ]
    }
   ],
   "source": [
    "# Query to select all rows from the table\n",
    "query = \"SELECT * FROM cademycode_students\"\n",
    "\n",
    "# Load data into a DataFrame\n",
    "df_students = pd.read_sql(query, engine)\n",
    "\n",
    "# Print the columns in the DataFrame\n",
    "print(f\"Columns in DataFrame after alteration:\")\n",
    "print(df_students.columns)\n",
    "print(df_students.info())\n",
    "\n",
    "print(df_students.head(20))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 134,
   "id": "fe64f49e-1471-4158-809f-7192c6f9082c",
   "metadata": {},
   "outputs": [],
   "source": [
    "cleaned_df_jobs = df_jobs.drop_duplicates()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 136,
   "id": "b5ec5c94-e160-45bb-804c-4f9f6989a274",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 4293 entries, 0 to 4292\n",
      "Data columns (total 13 columns):\n",
      " #   Column             Non-Null Count  Dtype  \n",
      "---  ------             --------------  -----  \n",
      " 0   uuid               4293 non-null   int64  \n",
      " 1   name               4293 non-null   object \n",
      " 2   dob                4293 non-null   object \n",
      " 3   sex                4293 non-null   object \n",
      " 4   contact_info       4293 non-null   object \n",
      " 5   job_id             4293 non-null   int64  \n",
      " 6   num_course_taken   4293 non-null   int64  \n",
      " 7   career_path_id     4293 non-null   int64  \n",
      " 8   time_spent_hrs     4293 non-null   float64\n",
      " 9   job_category       4293 non-null   object \n",
      " 10  avg_salary         4293 non-null   int64  \n",
      " 11  career_path_name   4293 non-null   object \n",
      " 12  hours_to_complete  4293 non-null   int64  \n",
      "dtypes: float64(1), int64(6), object(6)\n",
      "memory usage: 436.1+ KB\n",
      "None\n",
      "   uuid             name         dob sex  \\\n",
      "0     1  Annabelle Avery  1943-07-03   F   \n",
      "1     2      Micah Rubio  1991-02-07   M   \n",
      "2     3       Hosea Dale  1989-12-07   M   \n",
      "3     4     Mariann Kirk  1988-07-31   F   \n",
      "4     5  Lucio Alexander  1963-08-31   M   \n",
      "\n",
      "                                        contact_info  job_id  \\\n",
      "0  {\"mailing_address\": \"303 N Timber Key, Irondal...       7   \n",
      "1  {\"mailing_address\": \"767 Crescent Fair, Shoals...       7   \n",
      "2  {\"mailing_address\": \"P.O. Box 41269, St. Bonav...       7   \n",
      "3  {\"mailing_address\": \"517 SE Wintergreen Isle, ...       6   \n",
      "4  {\"mailing_address\": \"18 Cinder Cliff, Doyles b...       7   \n",
      "\n",
      "   num_course_taken  career_path_id  time_spent_hrs job_category  avg_salary  \\\n",
      "0                 6               1            4.99           HR       80000   \n",
      "1                 5               8            4.40           HR       80000   \n",
      "2                 8               8            6.74           HR       80000   \n",
      "3                 7               9           12.31    education       61000   \n",
      "4                14               3            5.64           HR       80000   \n",
      "\n",
      "            career_path_name  hours_to_complete  \n",
      "0             data scientist                 20  \n",
      "1          android developer                 27  \n",
      "2          android developer                 27  \n",
      "3  machine learning engineer                 35  \n",
      "4               data analyst                 12  \n"
     ]
    }
   ],
   "source": [
    "# merging DataFrames\n",
    "merged_df = pd.merge(df_students, cleaned_df_jobs, on='job_id', how='inner')\n",
    "merged_df = pd.merge(merged_df, df_courses, on='career_path_id', how='inner')\n",
    "print(merged_df.info())\n",
    "print(merged_df.head())\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "89e04c61-08dd-42d2-9745-1991f5a52157",
   "metadata": {},
   "outputs": [
    {
     "ename": "NameError",
     "evalue": "name 'merged_df' is not defined",
     "output_type": "error",
     "traceback": [
      "\u001b[1;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[1;31mNameError\u001b[0m                                 Traceback (most recent call last)",
      "Cell \u001b[1;32mIn[1], line 1\u001b[0m\n\u001b[1;32m----> 1\u001b[0m \u001b[38;5;28mprint\u001b[39m(\u001b[43mmerged_df\u001b[49m\u001b[38;5;241m.\u001b[39minfo())\n",
      "\u001b[1;31mNameError\u001b[0m: name 'merged_df' is not defined"
     ]
    }
   ],
   "source": [
    "print(merged_df.info())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 138,
   "id": "6896ea6a-5d56-4ca7-b28e-6c80bdb586dd",
   "metadata": {},
   "outputs": [],
   "source": [
    "merged_df.to_csv('merged_data.csv', index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a278ad60-b100-4282-828d-5e21ea50ab7d",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.1"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
