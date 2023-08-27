import requests
from bs4 import BeautifulSoup
import re
import csv
from collections import defaultdict
import pandas as pd
from datetime import datetime
from yarl import URL
from typing import List, Dict, Any
import os
from pollswebscraper import PollsWebScraper

class JoinOutputCSV:
  """
  Intake a list of dicts of scraped polls,
  and generate polls and trends CSVs.
  """

  def __init__(self) -> None:
    self.scrape_polls = PollsWebScraper()

  def generate_filename(self, base_name: str) -> str:
    """
    Generate a unique filename by appending a timestamp to the base name.
    Args:
      base_name: Base name of the file.
    Returns:
      str: Generated unique filename.
    """

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{base_name}_{timestamp}.csv"

  def check_and_merge(self, new_data: list, base_filename: str, join_on: str) -> None:
    """
    Check for an existing CSV file, and if it exists, merge it with the new data.
    Args:
      new_data: The new data to be saved.
      base_filename: The base name of the file to check.
      join_on: Column to perform join operation.
    """

    existing_files = [f for f in os.listdir('.') if os.path.isfile(f)]
    existing_csvs = [f for f in existing_files if base_filename in f and f.endswith('.csv')]

    if existing_csvs:
      most_recent_file = max(existing_csvs, key=os.path.getctime)
      self.outer_join_csvs(most_recent_file, new_data, self.generate_filename(base_filename), join_on)
    else:
      self.scrape_polls.export_to_csv(new_data, self.generate_filename(base_filename))

  def outer_join_csvs(self, old_csv_name: str, new_data: list, output_csv_name: str, join_on: str) -> None:
    """
    Perform an outer join on an existing CSV file with new data, and save it to a new CSV file.
    Args:
      old_csv_name: Name of the old CSV file.
      new_data: The new data as a list of dicts.
      output_csv_name: Name of the new CSV file to be created.
      join_on: Column name to join on.
    """

    # load the old .csv into a dataframe
    old_df = pd.read_csv(old_csv_name)

    # convert new_data to a dataframe
    new_df = pd.DataFrame(new_data)

    # perform outer join
    combined_df = pd.merge(old_df, new_df, how='outer', on=join_on)

    # combine the _x and _y columns into single columns
    for column in combined_df.columns:
      if column.endswith('_x'):
        combined_column = combined_df[column].combine_first(combined_df[column.replace('_x', '_y')])
        combined_df[column.replace('_x', '')] = combined_column
        combined_df.drop(columns=[column, column.replace('_x', '_y')], inplace=True)

    # dedupe
    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    # save the result to a new .csv file
    self.scrape_polls.export_to_csv(combined_df.to_dict('records'), output_csv_name)

  def polls_trends(self, url: URL, average_type: str) -> str:
    """
    Scrape polling website and output polls and trends CSV.
    Trends can be expressed either as a simple average polling figures
    per candidate per date if polls exist for that date for the candidate,
    or seven-day rolling average per date per candidate if data exists.
    Args:
      url: URL of the polling website we want to scrape.
      average_type: Can be 'Simple' or 'Rolling'.
    Returns:
      str: os paths of where the CVS (polls.csv and trends.csv) were saved.
    """

    scraped_polls_raw = self.scrape_polls.extract_table_data_from_url(url=url)
    scraped_polls = self.scrape_polls.clean_and_process_data(table_data=scraped_polls_raw)

    trends_simple = self.scrape_polls.calculate_date_avg(data=scraped_polls) # output is list of dicts
    self.check_and_merge(scraped_polls, "polls", join_on="Date")

    if average_type == 'Rolling':
      trends_rolling_df = self.scrape_polls.calculate_7d_rolling_avg(data=trends_simple) # output is a dataframe
      trends_rolling = trends_rolling_df.to_dict('records') # convert dataframe to list of dicts
      self.check_and_merge(trends_rolling, "trends", join_on="Date")
    else:
      self.check_and_merge(trends_simple, "trends", join_on="Date")
