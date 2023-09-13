# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -

import csv
import requests
import urllib.request
import zipfile
import os
from pathlib import Path


class MarketData:
    def __init__(self, url, output_folder):
        self.url = url
        self.output_folder = output_folder

    def extract(self):
        """
        This function extracts the banking data provided from PKDD.
        It downloads the ZIP file from the "url".
        
        Args:
            url (str): the URL containing the public data
        """
        try:
            response = requests.get(self.url, stream=True)
            zip_file_path, _ = urllib.request.urlretrieve(self.url)


            with open(zip_file_path, 'wb') as out_file:
                for chunk in response.iter_content(chunk_size=8192):
                    out_file.write(chunk)

            # Now, try to extract the ZIP file
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(self.output_folder)

            return zip_ref
        except Exception as e:
            print(f"Error, could not download data: {e}")
    
    def convert_asc_to_csv(self, district_column_names):
        """
        This function converts the .asc files to the .csv format.
        The function outputs a folder with a name from output_folder.
        This created folder will be in the current directory.

        Args:
            output_folder (str): the name of the folder where
            files will be stored

        """
        try:
            zip_ref = self.extract()
            # Process ASC files and convert them to CSV
            for file_name in zip_ref.namelist():
                if file_name.endswith(".asc"):
                    asc_path = os.path.join(self.output_folder, file_name)
                    csv_file_name = file_name[:-4] + ".csv"
                    csv_path = os.path.join(self.output_folder, csv_file_name)
                    with open(asc_path, "r") as asc_file, open(
                        csv_path, "w", newline=""
                    ) as csv_file:
                        asc_reader = csv.reader(asc_file, delimiter=";")
                        csv_writer = csv.writer(csv_file, delimiter=",")
                        if file_name == "district.asc":
                            next(asc_reader)
                            new_header = district_column_names
                            csv_writer.writerow(new_header)
                            csv_writer.writerows(asc_reader)
                        else:
                            for row in asc_reader:
                                csv_writer.writerow(row)
                    print(f"Converted {asc_path} to CSV.")
            print("All ASC files converted to CSV.")
        except Exception as e:
            print(f"Error, could not convert ASC to CSV: {e}")


if __name__ == "__main__":

    # Convert the current working directory to a Path object
    script_dir = Path(os.getcwd())
    data_dir = script_dir / 'etl'/ "expanded_data"

    # Columns to rename for district table
    district_column_names = [
            "district_id",
            "district_name",
            "region",
            "no_of_inhabitants",
            "no_of_municipalities_lt_499",
            "no_of_municipalities_500_1999",
            "no_of_municipalities_2000_9999",
            "no_of_municipalities_gt_10000",
            "no_of_cities",
            "ratio_of_urban_inhabitants",
            "average_salary",
            "unemployment_rate_95",
            "unemployment_rate_96",
            "no_of_entrepreneurs_per_1000_inhabitants",
            "no_of_committed_crimes_95",
            "no_of_committed_crimes_96",
        ]

    _ = MarketData("https://web.archive.org/web/20070214120527/http://lisp.vse.cz/pkdd99/DATA/data_berka.zip", data_dir)
    _.convert_asc_to_csv(district_column_names)
