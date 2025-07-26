from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import os
import logging
import json
import psycopg2
import glob

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'etl_postgres_test',
    default_args=default_args, 
    description='ETL DAG for Postgres',
    schedule_interval='@hourly',
    catchup=False,
    tags=['example', 'etl']
)

start_task = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

end_task = DummyOperator(
    task_id='End_execution',
    dag=dag
)


def get_db_connection():
    """Helper function to get database connection"""
    return psycopg2.connect("host=postgres dbname=airflow user=airflow password=airflow")

def stage_songs():
    conn = get_db_connection()
    cur = conn.cursor()
    nested_path = ['A', 'B', 'C']
    total_file_path = []
    total_records = []
    current_total = 0

    # Get all JSON files from song_data directory - use absolute path
    for nested in nested_path:
        total_file_path += glob.glob(f"/opt/airflow/data/song_data/A/A/{nested}/*.json")

    for nested in nested_path:
        total_file_path += glob.glob(f"/opt/airflow/data/song_data/A/B/{nested}/*.json")

    print(f"Found {len(total_file_path)} song files")

    # Create staging_songs table
    cur.execute("""
    DROP TABLE IF EXISTS public.staging_songs;
    CREATE TABLE public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );
    """)
    conn.commit()

    # Read and process each file
    for file_path in total_file_path:
        try:
            print(f"Processing song file: {file_path}")
            with open(file_path, 'r') as f:
                # Each line in the file is a separate JSON object
                for line in f:
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            record = json.loads(line)
                            total_records.append(record)
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON in file {file_path}: {e}")
                            print(f"Problematic line: {line}")
                            continue
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            continue

    print(f"Total song records to insert: {len(total_records)}")

    # Insert records into database
    for record in total_records:
        try:
            current_total += 1
            print(f'Loading song record {current_total}/{len(total_records)}')
            
            cur.execute("""
                INSERT INTO public.staging_songs(
                num_songs,
                artist_id,
                artist_name,
                artist_latitude,
                artist_longitude,
                artist_location,
                song_id,
                title,
                duration,
                year
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record.get('num_songs'),
                record.get('artist_id'),
                record.get('artist_name'),
                record.get('artist_latitude'),
                record.get('artist_longitude'),
                record.get('artist_location'),
                record.get('song_id'),
                record.get('title'),
                record.get('duration'),
                record.get('year'),
            ))
            
            # Commit every 100 records for better performance
            if current_total % 100 == 0:
                conn.commit()
                
        except Exception as e:
            print(f"Error inserting song record {current_total}: {e}")
            print(f"Record: {record}")
            conn.rollback()
            continue
    
    # Final commit
    conn.commit()
    print(f"Successfully loaded {current_total} song records")
    conn.close()

stage_songs_task = PythonOperator(
    task_id='stage_songs',
    python_callable=stage_songs,
    dag=dag
)

def stage_events():
    file_path = glob.glob("/opt/airflow/data/log_data/*.json")
    total_records = []
    current_total = 0
    conn = get_db_connection()
    cur = conn.cursor()
    
    print(f"Found {len(file_path)} event files")

    # Create staging_events table
    cur.execute("""
    DROP TABLE IF EXISTS public.staging_events;
    CREATE TABLE public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    """)
    conn.commit()

    # Read and process each file
    for file_name in file_path:
        try:
            print(f"Processing event file: {file_name}")
            with open(file_name, 'r') as f:
                # Each line in the file is a separate JSON object
                for line in f:
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            record = json.loads(line)
                            total_records.append(record)
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON in file {file_name}: {e}")
                            print(f"Problematic line: {line}")
                            continue
        except Exception as e:
            print(f"Error reading file {file_name}: {e}")
            continue

    print(f"Total event records to insert: {len(total_records)}")

    # Insert records into database
    for record in total_records:
        try:
            current_total += 1
            print(f'Loading event record {current_total}/{len(total_records)}')
            
            # Handle empty userId
            user_id = record.get('userId', '')
            if user_id == '' or user_id is None:
                user_id = 0
            else:
                try:
                    user_id = int(user_id)
                except (ValueError, TypeError):
                    user_id = 0
            
            cur.execute("""
                INSERT INTO public.staging_events(
                artist,
                auth,
                firstname,
                gender,
                iteminsession,
                lastname,
                length,
                level,
                location,
                method,
                page,
                registration,
                sessionid,
                song,
                status,
                ts,
                useragent,
                userid
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record.get('artist'),
                record.get('auth'),
                record.get('firstName'),
                record.get('gender'),
                record.get('itemInSession'),
                record.get('lastName'),
                record.get('length'),
                record.get('level'),
                record.get('location'),
                record.get('method'),
                record.get('page'),
                record.get('registration'),
                record.get('sessionId'),
                record.get('song'),
                record.get('status'),
                record.get('ts'),
                record.get('userAgent'),
                user_id
            ))
            
            # Commit every 100 records for better performance
            if current_total % 100 == 0:
                conn.commit()
                
        except Exception as e:
            print(f"Error inserting event record {current_total}: {e}")
            print(f"Record: {record}")
            conn.rollback()
            continue
    
    # Final commit
    conn.commit()
    print(f"Successfully loaded {current_total} event records")
    conn.close()

stage_events_task = PythonOperator(
    task_id='stage_events',
    python_callable=stage_events,
    dag=dag
)

def create_dimension_tables():
    """Create all dimension and fact tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create users table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            level varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
        """)
        
        # Create songs table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.songs (
            songid varchar(256) NOT NULL,
            title varchar(256),
            artistid varchar(256),
            year int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
        """)
        
        # Create artists table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.artists (
            artistid varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            lattitude numeric(18,0),
            longitude numeric(18,0),
            CONSTRAINT artists_pkey PRIMARY KEY (artistid)
        );
        """)
        
        # Create time table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.time (
            start_time timestamp NOT NULL,
            hour int4,
            day int4,
            week int4,
            month int4,
            year int4,
            weekday int4,
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );
        """)
        
        # Create songplays table with auto-increment ID
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.songplays (
            playid SERIAL PRIMARY KEY,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            level varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256)
        );
        """)
        
        conn.commit()
        logging.info("All tables created successfully")
        
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_dimension_tables,
    dag=dag
)

def load_songplays_fact_table():
    """Load data into the songplays fact table"""
    conn = get_db_connection()
    cur = conn.cursor()
    logging.info("Loading songplays fact table")
    
    try:
        # Clear the table first
        cur.execute("DELETE FROM songplays")
        
        # Insert data with auto-increment ID, no need for manual playid
        insert_query = """
        INSERT INTO songplays (start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                ts,
                *
            FROM public.staging_events
            WHERE page = 'NextSong' AND userid != 0
        ) AS events
        LEFT JOIN public.staging_songs AS songs
        ON LOWER(TRIM(events.song)) = LOWER(TRIM(songs.title))
        AND LOWER(TRIM(events.artist)) = LOWER(TRIM(songs.artist_name))
        AND ABS(COALESCE(events.length, 0) - COALESCE(songs.duration, 0)) < 2.0  -- Allow 2 second difference
        """
        
        cur.execute(insert_query)
        
        # Check how many records were inserted
        cur.execute("SELECT COUNT(*) FROM songplays")
        count = cur.fetchone()[0]
        logging.info(f"Inserted {count} records into songplays table")
        
        # Check how many have matched songs
        cur.execute("SELECT COUNT(*) FROM songplays WHERE songid IS NOT NULL")
        matched_count = cur.fetchone()[0]
        logging.info(f"Records with matched songs: {matched_count}")
        
        conn.commit()
        logging.info("Songplays fact table loaded successfully")
        
    except Exception as e:
        logging.error(f"Error in load_songplays_fact_table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

load_songplays_fact_table_task = PythonOperator(
    task_id='load_songplays_fact_table',
    python_callable=load_songplays_fact_table,
    dag=dag
)

def load_user_dim_table():
    """Load data into the user dimension table"""
    conn = get_db_connection()
    cur = conn.cursor()
    logging.info("Loading user dimension table")
    
    try:
        # First clear the table
        cur.execute("DELETE FROM users")
        
        # Insert data with direct SQL query
        insert_query = """
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT DISTINCT userid, firstname, lastname, gender, level
        FROM public.staging_events
        WHERE page = 'NextSong' 
        AND userid IS NOT NULL 
        AND userid != 0
        """
        
        cur.execute(insert_query)
        
        # Check how many records were inserted
        cur.execute("SELECT COUNT(*) FROM users")
        count = cur.fetchone()[0]
        logging.info(f"Inserted {count} records into users table")
        
        conn.commit()
        logging.info("User dimension table loaded successfully")
        
    except Exception as e:
        logging.error(f"Error in load_user_dim_table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

load_user_dim_table_task = PythonOperator(
    task_id='load_user_dim_table',
    python_callable=load_user_dim_table,
    dag=dag
)   

def load_song_dim_table():
    """Load data into the song dimension table"""
    conn = get_db_connection()
    cur = conn.cursor()
    logging.info("Loading song dimension table")
    
    try:
        # First clear the table
        cur.execute("DELETE FROM songs")
        
        # Insert data with direct SQL query
        insert_query = """
        INSERT INTO songs (songid, title, artistid, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM public.staging_songs
        WHERE song_id IS NOT NULL
        """
        
        cur.execute(insert_query)
        
        # Check how many records were inserted
        cur.execute("SELECT COUNT(*) FROM songs")
        count = cur.fetchone()[0]
        logging.info(f"Inserted {count} records into songs table")
        
        conn.commit()
        logging.info("Song dimension table loaded successfully")
        
    except Exception as e:
        logging.error(f"Error in load_song_dim_table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

load_song_dim_table_task = PythonOperator(
    task_id='load_song_dim_table',
    python_callable=load_song_dim_table,
    dag=dag
)

def load_artist_dim_table():
    """Load data into the artist dimension table"""
    conn = get_db_connection()
    cur = conn.cursor()
    logging.info("Loading artist dimension table")
    
    try:
        # First clear the table
        cur.execute("DELETE FROM artists")
        
        # Insert data with direct SQL query
        insert_query = """
        INSERT INTO artists (artistid, name, location, lattitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM public.staging_songs
        WHERE artist_id IS NOT NULL
        """
        
        cur.execute(insert_query)
        
        # Check how many records were inserted
        cur.execute("SELECT COUNT(*) FROM artists")
        count = cur.fetchone()[0]
        logging.info(f"Inserted {count} records into artists table")
        
        conn.commit()
        logging.info("Artist dimension table loaded successfully")
        
    except Exception as e:
        logging.error(f"Error in load_artist_dim_table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

load_artist_dim_table_task = PythonOperator(
    task_id='load_artist_dim_table',
    python_callable=load_artist_dim_table,
    dag=dag
)   

def load_time_dim_table():
    """Load data into the time dimension table"""
    conn = get_db_connection()
    cur = conn.cursor()
    logging.info("Loading time dimension table")
    
    try:
        # First clear the table
        cur.execute("DELETE FROM time")
        
        # Insert data with direct SQL query
        insert_query = """
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            start_time, 
            extract(hour from start_time) as hour, 
            extract(day from start_time) as day, 
            extract(week from start_time) as week, 
            extract(month from start_time) as month, 
            extract(year from start_time) as year, 
            extract(dow from start_time) as weekday
        FROM public.songplays
        WHERE start_time IS NOT NULL
        """
        
        cur.execute(insert_query)
        
        # Check how many records were inserted
        cur.execute("SELECT COUNT(*) FROM time")
        count = cur.fetchone()[0]
        logging.info(f"Inserted {count} records into time table")
        
        conn.commit()
        logging.info("Time dimension table loaded successfully")
        
    except Exception as e:
        logging.error(f"Error in load_time_dim_table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

load_time_dim_table_task = PythonOperator(
    task_id='load_time_dim_table',
    python_callable=load_time_dim_table,
    dag=dag
)

def run_quality_checks():
    """Run data quality checks on all tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    logging.info("Running quality checks")
    
    try:
        tables = ['staging_songs', 'staging_events', 'songplays', 'users', 'songs', 'artists', 'time']
        
        for table in tables:
            # Check if table is empty
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            if count < 1:
                raise ValueError(f"Quality check failed: {table} table is empty")
            
            logging.info(f"Quality check passed: {table} table has {count} records")
            
            # Additional quality checks based on table
            if table == 'songplays':
                # Check for NULL primary keys (should not happen with SERIAL)
                cur.execute("SELECT COUNT(*) FROM songplays WHERE playid IS NULL")
                null_count = cur.fetchone()[0]
                if null_count > 0:
                    raise ValueError(f"Quality check failed: {null_count} NULL playid values in songplays table")
                
                # Check for NULL start_time
                cur.execute("SELECT COUNT(*) FROM songplays WHERE start_time IS NULL")
                null_start_time = cur.fetchone()[0]
                if null_start_time > 0:
                    raise ValueError(f"Quality check failed: {null_start_time} NULL start_time values in songplays table")
                    
            elif table == 'users':
                # Check for NULL user IDs
                cur.execute("SELECT COUNT(*) FROM users WHERE userid IS NULL")
                null_count = cur.fetchone()[0]
                if null_count > 0:
                    raise ValueError(f"Quality check failed: {null_count} NULL userid values in users table")
                    
            elif table == 'songs':
                # Check for NULL song IDs
                cur.execute("SELECT COUNT(*) FROM songs WHERE songid IS NULL")
                null_count = cur.fetchone()[0]
                if null_count > 0:
                    raise ValueError(f"Quality check failed: {null_count} NULL songid values in songs table")
                    
            elif table == 'artists':
                # Check for NULL artist IDs
                cur.execute("SELECT COUNT(*) FROM artists WHERE artistid IS NULL")
                null_count = cur.fetchone()[0]
                if null_count > 0:
                    raise ValueError(f"Quality check failed: {null_count} NULL artistid values in artists table")
                    
            elif table == 'time':
                # Check for NULL start_time
                cur.execute("SELECT COUNT(*) FROM time WHERE start_time IS NULL")
                null_count = cur.fetchone()[0]
                if null_count > 0:
                    raise ValueError(f"Quality check failed: {null_count} NULL start_time values in time table")
        
        logging.info("All quality checks passed successfully")
        
    except Exception as e:
        logging.error(f"Quality check failed: {e}")
        raise
    finally:
        conn.close()

run_quality_checks_task = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_quality_checks,
    dag=dag
) 

# Task dependencies
start_task >> create_tables_task >> [stage_songs_task, stage_events_task] >> load_songplays_fact_table_task >> [load_user_dim_table_task, load_song_dim_table_task, load_artist_dim_table_task] >> load_time_dim_table_task >> run_quality_checks_task >> end_task