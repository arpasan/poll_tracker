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

class PollsWebScraper:
  """
  Ping a page, and scrape a website containing polling data.
  """

  def __init__(self) -> None:
    pass

  def extract_table_data_from_url(self, url: URL) -> list:
    """
    Args:
      url: Intakes a URL.
    Returns:
      Outputs a correctly formatted list for further processing.
    """

    result = []

    try:
      response = requests.get(url)
      response.raise_for_status() # raise exception if status indicates error
    except requests.exceptions.RequestException as e:
      print("An erro occurred while requesting the URL:", str(e))
      return result
    
    try:
      html_content = response.text
      soup = BeautifulSoup(html_content, 'html.parser')

      table = soup.find('table')
      if table is None:
        return result

      # extract column names
      column_names = []
      for th in table.find_all('th'):
          column_names.append(th.text.strip())

      # extract rows
      rows = table.find_all('tr')

      for row in rows:
          columns = row.find_all('td')
          if len(columns) > 0:
              row_data = {}
              for i in range(min(len(column_names), len(columns))):
                  row_data[column_names[i]] = columns[i].text.strip()
              result.append(row_data)

    except Exception as e:
      print("An error occurred while parsing the HTML:", str(e))
      return result
    
    return result

  def is_percentage_column(self, data: List[Dict[str, Any]], column_name: str) -> bool:
    """
    Check if a column typically contains percentage values.
    This comes in handy in post-processing.
    Args:
      data: Raw extract in the form of a list of dicts.
      column_name: Key from data[0].keys().
    Returns:
      Boolean -> is a percentage col. True / False.
    """

    for row in data:
        if '%' in row[column_name]:
            return True
    return False

  def clean_and_process_data(self, table_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Cleans and processes the raw extract to prep it for outputting to CSV.
    Args:
      data: Raw extract in the form of a list of dicts.
    Returns:
      List of dicts, which is the cleaned-up scraped data.
    """

    cleaned_data = []

    # identify which columns typically contain percentages
    percentage_columns = [key for key in table_data[0].keys() if self.is_percentage_column(table_data, key)]

    for row in table_data:
        new_row = {}
        for key, value in row.items():
            # specific cleaning for pollster column
            if key == "Pollster":
                cleaned_value = re.sub(r'[*]', '', value) # only remove asterisks
            else:
                cleaned_value = re.sub(r'[*,\s]', '', value) # remove asterisks, spaces, and commas

            # convert percentage values (e.g., '99.4%') to decimal format (e.g., 0.994)
            if '%' in cleaned_value:
                new_row[key] = float(cleaned_value.replace('%', '')) / 100
            elif key in percentage_columns:
                # handle integer percentages, e.g., '33' as 0.33
                try:
                    if '.' in cleaned_value: # a float, so handle it as a percentage
                        new_row[key] = float(cleaned_value) / 100
                    else:  # It's likely an integer
                        possible_int_percentage = int(cleaned_value)
                        new_row[key] = possible_int_percentage / 100 # treat as a percentage
                except ValueError:
                    new_row[key] = cleaned_value  # if not percentage, store the cleaned string
            # try to convert other values to integer or float, or just store cleaned string
            else:
                try:
                    new_row[key] = int(cleaned_value)
                except ValueError:
                    try:
                        new_row[key] = float(cleaned_value)
                    except ValueError:
                        new_row[key] = cleaned_value

        cleaned_data.append(new_row)

    return cleaned_data

  def export_to_csv(self, data: List[Dict[str, Any]], filename: str) -> str:
    """
    Export provided data to a CSV file.

    Args:
        data: List of dicts containing data to be written to CSV.
        filename: Name of the CSV file to be created.
    Returns:
        str: The file path of the created CSV file.
    """
    # extract the set of all fields from all rows
    fieldnames = set().union(*(d.keys() for d in data))

    # define a custom sorting function
    def custom_sort(key):
        priority_order = ['Date', 'Pollster', 'Sample']
        try:
            # assign priority based on the order in the priority_order list
            # lower index indicates higher priority
            return (priority_order.index(key),)
        except ValueError:
            # if key isn't in priority_order, use the column name itself for sorting
            return (len(priority_order), key)

    fieldnames = sorted(fieldnames, key=custom_sort)

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # return the path of the created file.
    return print(os.path.abspath(filename)) # filename

  # date avg. for a given candidate on a given date if data exists
  def calculate_date_avg(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Calculate polls' average per candiate on a given date if data exists.
    Args:
      data: Cleaned-up extract in the form of a list of dicts.
    Returns:
      List of dicts but with the average polling figure per polling date per candidate.
    """

    averages = defaultdict(lambda: defaultdict(list))

    # populate the averages dictionary
    for row in data:
        date = row["Date"]
        for key, value in row.items():
            if key not in ["Date", "Pollster", "Sample"] and value != '':
                # only add to the list if the value is not empty
                averages[date][key].append(value)

    trend_data = []
    for date, candidates in averages.items():
        avg_row = {"Date": date}
        for candidate, values in candidates.items():
            avg_row[candidate] = sum(values) / len(values) if values else '' # only calculate average if values is not empty
        trend_data.append(avg_row)

    # sort by date
    def convert_to_date(date_str):
      return datetime.strptime(date_str, '%m/%d/%y')

    trend_data = sorted(trend_data, key=lambda x: convert_to_date(x['Date']))

    return trend_data

  def calculate_7d_rolling_avg(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Calculate seven-day rolling average of the polling data.
    Args:
      data: Cleaned-up polls' simple averages in the form of a list of dicts.
    Returns:
      Dataframe with seven-day rolling average per day per candidate if data exists.
    """

    # convert to dataframe
    trend_df = pd.DataFrame(data)

    # convert 'date' to date type and sort by it
    trend_df['Date'] = pd.to_datetime(trend_df['Date'])
    trend_df = trend_df.sort_values(by='Date')

    # calc. 7-day rolling average for each candidate
    candidates = [col for col in trend_df.columns if col not in ['Date']]

    for candidate in candidates:
        trend_df[f'{candidate}'] = trend_df[candidate].rolling(window=7).mean()

    trend_df['Date'] = trend_df['Date'].dt.strftime('%m/%d/%y') # align the formatting

    return trend_df
