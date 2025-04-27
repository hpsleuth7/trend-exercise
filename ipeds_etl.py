# define class ipedsETLObject
import pandas as pd
import numpy as np
import os
import requests
import zipfile
import io
import logging
import sqlalchemy
import sys

from datetime import datetime
from importlib import resources
import json
import typing

class ipedsETLObject(object):
    """
    A class to handle the extraction, transformation, and loading (ETL) of IPEDS data.
    
    Attributes:
        base_url (str): The base URL for the IPEDS data.
        data_dir (str): The directory where the data will be stored.
        logger (logging.Logger): Logger for the class.
    """

    def __init__(self, base_url:str, data_dir:str = None):
        
        self.base_url = base_url
        if data_dir is None:
            self.data_dir = os.path.join(os.getcwd(), 'data')
    
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        # Create a file handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        # Test the logger
        self.logger.info("Logger initialized.")
    
    def download_data(self) -> None:
        """
        Download the IPEDS data from the base URL and save it to the data directory.
        """
        self.logger.info(f"Downloading data from {self.base_url}")
        
        # Create the data directory if it doesn't exist
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
        # Download the institution records to the data directory
        student_charges_url = f"{self.base_url}/HD2023.zip"
        response = requests.get(student_charges_url)
        if response.status_code == 200:
            with open(os.path.join(self.data_dir, 'institution.zip'), 'wb') as f:
                f.write(response.content)
            self.logger.info("Institution records downloaded successfully.")
        else:
            self.logger.error(f"Failed to download institution records. Status code: {response.status_code}")
        


        # Download the student charges table to the data directory
        student_charges_url = f"{self.base_url}/IC2023_AY.zip"
        response = requests.get(student_charges_url)
        if response.status_code == 200:
            with open(os.path.join(self.data_dir, 'student_charges.zip'), 'wb') as f:
                f.write(response.content)
            self.logger.info("Student charges data downloaded successfully.")
        else:
            self.logger.error(f"Failed to download student charges data. Status code: {response.status_code}")
        
        # Download the admissions table
        admissions_url = f"{self.base_url}/ADM2023.zip"
        response = requests.get(admissions_url)
        if response.status_code == 200:
            with open(os.path.join(self.data_dir, 'admissions.zip'), 'wb') as f:
                f.write(response.content)
            self.logger.info("Admissions data downloaded successfully.")
        else:
            self.logger.error(f"Failed to download admissions data. Status code: {response.status_code}")

        # Download the completions table
        completions_url = f"{self.base_url}/C2023_B.zip"
        response = requests.get(completions_url)
        if response.status_code == 200:
            with open(os.path.join(self.data_dir, 'completions.zip'), 'wb') as f:
                f.write(response.content)
            self.logger.info("Completions data downloaded successfully.")
        else:
            self.logger.error(f"Failed to download completions data. Status code: {response.status_code}")

        # Download the enrollment table
        enrollment_url = f"{self.base_url}/EF2023C.zip"
        response = requests.get(enrollment_url)
        if response.status_code == 200:
            with open(os.path.join(self.data_dir, 'enrollment.zip'), 'wb') as f:
                f.write(response.content)
            self.logger.info("Enrollment data downloaded successfully.")
        else:
            self.logger.error(f"Failed to download enrollment data. Status code: {response.status_code}")

    def read_data(self) -> None:
        """
        Load the downloaded data from the data directory and store it in pandas DataFrames.
        """
        self.logger.info("Loading data...")
        
        # Unzip the downloaded files
        for file in os.listdir(self.data_dir):
            if file.endswith('.zip'):
                with zipfile.ZipFile(os.path.join(self.data_dir, file), 'r') as zip_ref:
                    zip_ref.extractall(self.data_dir)
                self.logger.info(f"Extracted {file} to {self.data_dir}")
        
        # Load the data into pandas DataFrames
        self.institution_df = pd.read_csv(os.path.join(self.data_dir, 'HD2023.csv'),
                                          encoding='latin1')
        self.student_charges_df = pd.read_csv(os.path.join(self.data_dir, 'IC2023_AY.csv'))
        self.admissions_df = pd.read_csv(os.path.join(self.data_dir, 'ADM2023.csv'))
        self.completions_df = pd.read_csv(os.path.join(self.data_dir, 'C2023_B.csv'))
        self.enrollment_df = pd.read_csv(os.path.join(self.data_dir, 'EF2023C.csv'))
        self.logger.info("Data loaded successfully.")
    
    def transform_data(self) -> None:
        """
        Transform the data as needed.
        """
        # read in metadata/admissions_cols.json
        with open('metadata/admissions_cols.json', 'r') as f:
            admissions_cols = json.load(f)
        # rename admissions_df using admissions_cols
        self.admissions_df.rename(columns=admissions_cols, inplace=True)
        self.admissions_df = self.admissions_df[admissions_cols.values()]

        with open('metadata/completions_cols.json', 'r') as f:
            completions_cols = json.load(f)
        # rename completions_df using completions_cols
        self.completions_df.rename(columns=completions_cols, inplace=True)
        self.completions_df = self.completions_df[completions_cols.values()]

        with open('metadata/enrollment_cols.json', 'r') as f:
            enrollment_cols = json.load(f)
        # rename enrollment_df using enrollment_cols
        import pdb
        pdb.set_trace()
        self.enrollment_df.rename(columns=enrollment_cols, inplace=True)
        self.enrollment_df = self.enrollment_df[enrollment_cols.values()]

        with open('metadata/institution_cols.json', 'r') as f:
            institution_cols = json.load(f)
        # rename institution_df using institution_cols
        self.institution_df.rename(columns=institution_cols, inplace=True)
        self.institution_df = self.institution_df[institution_cols.values()]

        with open('metadata/student_charges_cols.json', 'r') as f:
            student_charges_cols = json.load(f)
        # rename student_charges_df using student_charges_cols
        self.student_charges_df.rename(columns=student_charges_cols, inplace=True)
        self.student_charges_df = self.student_charges_df[student_charges_cols.values()]

        # Map values
        # set admissions_df['consider_work_experience'] to mapping:
            # 1.0 -> 'Required for admission'
            # 3.0 -> 'Not considered for admission'
            # 5.0 -> 'Considered for admission'
        self.admissions_df['consider_work_exp'] = self.admissions_df['consider_work_exp'].map({
            1.0: 'Required for admission',
            3.0: 'Not considered for admission',
            5.0: 'Considered for admission'
        })
    
    def load_data(self) -> None:
        """
        Load the data into a database.
        """
        self.logger.info("Loading data into database...")
        
        # Create a database connection
        engine = sqlalchemy.create_engine('sqlite:///ipeds_data.db')
        
        # Load the data into the database
        self.institution_df.to_sql('institution', engine, if_exists='replace', index=False)
        self.student_charges_df.to_sql('student_charges', engine, if_exists='replace', index=False)
        self.admissions_df.to_sql('admissions', engine, if_exists='replace', index=False)
        self.completions_df.to_sql('completions', engine, if_exists='replace', index=False)
        self.enrollment_df.to_sql('enrollment', engine, if_exists='replace', index=False)
        
        
        self.logger.info("Data loaded into database successfully.")
