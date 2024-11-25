import logging
import pymysql
import pandas as pd
from config import DB_CONFIG

class DatabaseConnection:
    def __init__(self):
        logging.info("Establishing database connection")
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=DB_CONFIG['server'],
                user=DB_CONFIG['username'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                port=DB_CONFIG['port'],
                connect_timeout=10
            )
            logging.info("Database connection successful!")
            self.cursor = self.connection.cursor()
        except pymysql.MySQLError as e:
            logging.error(f"MySQL Error: {e}")

    def disconnect(self):
        """Close the database connection."""
        try:
            if self.connection and self.connection.open:
                self.connection.close()
                logging.info("Database connection closed.")
        except Exception as e:
            logging.error(f"Error while closing connection: {e}")

    def check_table_exists(self, table_name):
        """Check if the table already exists in the database."""
        query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        AND table_schema = DATABASE();
        """
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result[0] > 0
        except pymysql.MySQLError as e:
            logging.error(f"Error checking if table exists: {e}")
            return False

    def execute_query(self, query, params=None):
        """Execute a query with optional parameters."""
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
        except pymysql.MySQLError as e:
            logging.error(f"Error executing query: {e}")

    def fetch_dataframe(self, sql_query):
        """Fetch data from the database and return as a DataFrame."""
        try:
            df = pd.read_sql(sql_query, self.connection)
            return df
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            return None
