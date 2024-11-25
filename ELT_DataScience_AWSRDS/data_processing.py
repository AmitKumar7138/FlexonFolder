import logging
import pandas as pd
import pymysql
from database_connection import DatabaseConnection
from config import S3_CLIENT, AWS_CONFIG

class DataProcessing:
    def __init__(self):
        self.db = DatabaseConnection()
        self.bucket_name = AWS_CONFIG['bucket_name']

    def _create_table(self, df, table_name):
        """Create a table from DataFrame columns."""
        if not self.db.cursor:
            logging.error("Cursor not available")
            return

        columns = []
        for col, dtype in df.dtypes.items():
            if 'int' in str(dtype):
                columns.append(f"`{col}` INT")
            elif 'float' in str(dtype):
                columns.append(f"`{col}` FLOAT")
            else:
                columns.append(f"`{col}` VARCHAR(255)")

        create_table_query = f"""
        CREATE TABLE {table_name} ({', '.join(columns)});
        """
        try:
            self.db.execute_query(create_table_query)
            logging.info(f"Table '{table_name}' created successfully!")
        except Exception as e:
            logging.error(f"Error creating table: {e}")

    def _insert_into_table(self, df, table_name):
        """Insert data from DataFrame into the table."""
        # Replace NaN with None
        df = df.where(pd.notnull(df), None)

        columns = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        try:
            for _, row in df.iterrows():
                self.db.cursor.execute(insert_query, tuple(row))
            self.db.connection.commit()
            logging.info(f"Data inserted into table '{table_name}' successfully!")
        except pymysql.MySQLError as e:
            logging.error(f"Error inserting data: {e}")

    def create_table_from_csv(self, path, table_name):
        """Create a table from a CSV file."""
        df = pd.read_csv(path)
        logging.info(f"Data read from CSV '{path}'")
        if not self.db.check_table_exists(table_name):
            self._create_table(df, table_name)
            self._insert_into_table(df, table_name)
        else:
            logging.info(f"Table '{table_name}' already exists.")

    def data_preprocessing(self, table_name):
        """Fetch data from the table and perform preprocessing."""
        sql_query = f"SELECT * FROM {table_name}"
        df = self.db.fetch_dataframe(sql_query)
        if df is not None:
            logging.info(f"Data fetched from table '{table_name}'")
            # Upload the original data to S3
            self.upload_to_s3(df, f"{table_name}.csv", "original_data")
            # Proceed with preprocessing
            self.drop_na(df, table_name)
            self.group_by(df, table_name)
            self.pivot(df, table_name)
            self.melt(df, table_name)
            self.stack(df, table_name)
            self.merge(df, table_name)
            self.concat(df, table_name)
            self.union(df, table_name)
        else:
            logging.error("Failed to fetch data for preprocessing.")

    def upload_to_s3(self, df, file_name, folder_name):
        """Upload a DataFrame to S3 as a CSV file."""
        csv_buffer = df.to_csv(index=False)
        s3_key = f"{folder_name}/{file_name}"
        try:
            S3_CLIENT.put_object(Bucket=self.bucket_name, Key=s3_key, Body=csv_buffer)
            logging.info(f"Uploaded {file_name} to S3 bucket {self.bucket_name} in folder {folder_name}.")
        except Exception as e:
            logging.error(f"Failed to upload {file_name} to S3: {e}")

    ### Preprocessing steps
    def drop_na(self, df, table_name):
        """Drop missing values and insert into a new table."""
        df_drop_na = df.dropna()
        new_table_name = f"{table_name}_DropNa"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_drop_na, new_table_name)
            self._insert_into_table(df_drop_na, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_drop_na, f"{new_table_name}.csv", "processed_data")

    def group_by(self, df, table_name):
        df_groupby = df.groupby(['Pclass', 'Sex'])['Survived'].mean().reset_index()
        df_groupby.rename(columns={'Survived': 'Survival_Rate'}, inplace=True)
        new_table_name = f"{table_name}_GroupBy"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_groupby, new_table_name)
            self._insert_into_table(df_groupby, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_groupby, f"{new_table_name}.csv", "processed_data")

    def pivot(self, df, table_name):
        df_unique = df.drop_duplicates(subset=['Pclass', 'Sex'])
        df_pivot = df_unique.pivot(index='Pclass', columns='Sex', values='Fare').reset_index()
        new_table_name = f"{table_name}_Pivot"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_pivot, new_table_name)
            self._insert_into_table(df_pivot, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_pivot, f"{new_table_name}.csv", "processed_data")

    def melt(self, df, table_name):
        df_melt = pd.melt(
            df,
            id_vars=['PassengerId', 'Pclass', 'Sex', 'Survived'],
            value_vars=['Age', 'Fare', 'SibSp', 'Parch'],
            var_name='Measurement',
            value_name='Value'
        )
        new_table_name = f"{table_name}_Melt"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_melt, new_table_name)
            self._insert_into_table(df_melt, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_melt, f"{new_table_name}.csv", "processed_data")

    def stack(self, df, table_name):
        df_subset = df[['PassengerId', 'Pclass', 'Sex', 'Age', 'Fare', 'SibSp', 'Parch']]
        df_subset.set_index(['PassengerId', 'Pclass', 'Sex'], inplace=True)
        df_stack = df_subset.stack().reset_index(name='Value')
        df_stack.rename(columns={'level_3': 'Measurement'}, inplace=True)
        new_table_name = f"{table_name}_Stack"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_stack, new_table_name)
            self._insert_into_table(df_stack, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_stack, f"{new_table_name}.csv", "processed_data")

    def merge(self, df, table_name):
        merged_df = pd.merge(
            df,
            df,
            on='Ticket',
            suffixes=('_left', '_right')
        )
        df_merge = merged_df[merged_df['PassengerId_left'] != merged_df['PassengerId_right']]
        new_table_name = f"{table_name}_Merge"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_merge, new_table_name)
            self._insert_into_table(df_merge, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_merge, f"{new_table_name}.csv", "processed_data")

    def concat(self, df, table_name):
        df_male = df[df['Sex'] == 'male']
        df_female = df[df['Sex'] == 'female']
        df_concat = pd.concat([df_male, df_female], axis=0, ignore_index=True)
        new_table_name = f"{table_name}_Concat"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_concat, new_table_name)
            self._insert_into_table(df_concat, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_concat, f"{new_table_name}.csv", "processed_data")

    def union(self, df, table_name):
        df_subset = df[['PassengerId', 'Pclass', 'Sex']]
        df_union = pd.concat([df, df_subset], axis=0).drop_duplicates()
        new_table_name = f"{table_name}_Union"
        if not self.db.check_table_exists(new_table_name):
            self._create_table(df_union, new_table_name)
            self._insert_into_table(df_union, new_table_name)
        # Upload to S3
        self.upload_to_s3(df_union, f"{new_table_name}.csv", "processed_data")

    def close_connection(self):
        self.db.disconnect()
