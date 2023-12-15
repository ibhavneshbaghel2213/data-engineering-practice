import requests
from zipfile import ZipFile
import os
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

downloads_path = "downloads"
os.makedirs(downloads_path, exist_ok=True)

def download_file(link) :
    try:
        response = requests.get(link)
        if response.status_code == 200:
            filename = os.path.join(downloads_path, link.split('/')[-1])
            with open(filename, 'wb') as f:
                f.write(response.content)
        else:
            print(f"Invalid URL: {link}")
    except Exception as e:
        print(f"Error downloading {link}: {e}")

def main():
    with ThreadPoolExecutor(max_workers=4) as excutor :
        excutor.map(download_file,download_uris)

    for file in os.listdir(downloads_path):
        file_path = os.path.join(downloads_path, file)
        if file.endswith(".zip"):
            with ZipFile(file_path, 'r') as zip_file:
                zip_file.extract(f"{os.path.splitext(file)[0]}.csv", path=downloads_path)
            os.remove(file_path)
    pass


if __name__ == "__main__":
    main()
