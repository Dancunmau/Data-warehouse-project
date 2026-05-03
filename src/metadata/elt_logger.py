"""
elt_logger.py
-------------
A metadata tracking module that logs pipeline executions natively into PostgreSQL.
This provides 'Operational Metadata', answering 'when, how, and by whom' data was processed.
"""

import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_db_engine():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    return create_engine(URL.create(
        "postgresql",
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "postgres"),
    ))

class ETLLogger:
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.end_time = None
        self.status = "RUNNING"
        self.records_read = 0
        self.records_written = 0
        self.engine = get_db_engine()

    def complete(self, status="SUCCESS", read=0, written=0, error_message=None):
        self.end_time = datetime.now()
        self.status = status
        self.records_read = read
        self.records_written = written
        self._write_log(error_message)
        
    def fail(self, read=0, error_message=None):
        self.complete(status="FAILED", read=read, written=0, error_message=error_message)
        
    def _write_log(self, error_message):
        try:
            with self.engine.connect() as conn:
                # Ensure the table is created by running the DDL if it doesn't exist
                ddl_path = os.path.join(BASE_DIR, "sql", "metadata", "create_metadata_tables.sql")
                with open(ddl_path, "r") as f:
                    conn.execute(text(f.read()))
                
                # Insert the Operational Metadata Log
                query = text("""
                    INSERT INTO etl_job_execution 
                    (job_name, start_time, end_time, status, records_read, records_written, error_message)
                    VALUES (:job, :start, :end, :status, :read, :written, :err)
                """)
                conn.execute(query, {
                    "job": self.job_name,
                    "start": self.start_time,
                    "end": self.end_time,
                    "status": self.status,
                    "read": self.records_read,
                    "written": self.records_written,
                    "err": error_message
                })
                conn.commit()
                print(f"[Metadata] Logged {self.status} for job '{self.job_name}' natively to PostgreSQL.")
        except Exception as e:
            print(f"[Metadata Error] Failed to write operational metadata to Postgres: {e}")

# Example Usage:
if __name__ == "__main__":
    logger = ETLLogger("demo_el_job")
    logger.complete(status="SUCCESS", read=500, written=490)
