
import requests
import msal
import os
import logging
import pandas as pd
import numpy as np
from io import BytesIO
import base64
import json
import sys
import instaloader
import pandas as pd
import re
import time
from yt_dlp import YoutubeDL
from datetime import datetime
import re
import json
import urllib.request as req
from datetime import datetime


# from .blob import Blob
# from .sql import SQLServerHandler
# from .car_analysis_pipeline import CarVisualizationGenerator
# from .car_analysis_pipeline_new import CarVisualizationGenerator
# from .reference import car_names,car_names_EV

#from reference import column_list

from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv(override=True)


WEBHOOK_URL = "https://tatamotors.webhook.office.com/webhookb2/79765211-9609-497c-b52c-8e4d2596a8a1@a3fb180d-23c4-44c9-a241-c78109202bd3/IncomingWebhook/b99e3ad5e280445b920d61ce1c5a3035/c42832ed-4d77-4efb-946b-a7169104a6bd/V2iVN1dWkxDOlN0dK_nTUvbSZqMbWvf6dfvJdMxx5Ihvo1"
THRESHOLD = 1000


def post_message_to_teams(message: str) -> None:
    try:
        request = req.Request(url=WEBHOOK_URL, method="POST")
        request.add_header("Content-Type", "application/json")
        data = json.dumps({"text": message}).encode()
        with req.urlopen(request, data=data) as response:
            if response.status != 200:
                raise Exception(f"Error: {response.reason}")
            print("Message sent successfully to Teams.")
    except Exception as e:
        print(f"An error occurred: {e}")



class ProcessPipeline:

    def __init__(self) -> None:
        self.client_id = os.getenv("client_id")
        self.tenant_id = os.getenv("tenant_id")
        self.sender_email = os.getenv("sender_email")
        # self.recipients = recipients
        # self.cc_recipients = cc_recipients

        self.scope = ["https://graph.microsoft.com/.default"]

        self.client_secret = os.getenv("client_secret")

        self.blob_connection_string = os.getenv("blob-storage-connection-string")
        self.container_name = os.getenv("date_container_name")
        self.container_name_temp_images = os.getenv("temp_data")

        # self.hostname = "tatamotors.sharepoint.com"
        self.hostname = "tatamotors.sharepoint.com"
        self.site_name = "Sentiment_Data"
        self.folder_path = "/Campaign_Tracking_Links/"

        # self.blob = Blob(self.blob_connection_string, self.container_name)
        # self.blob_temp = Blob(
        #     self.blob_connection_string, self.container_name_temp_images
        # )

        # self.sql_server = SQLServerHandler(
        #     server=os.getenv("server"),
        #     database=os.getenv("database"),
        #     username=os.getenv("user"),
        #     password=os.getenv("password"),
        # )

    def get_access_token(self):
        authority_url = f"https://login.microsoftonline.com/{self.tenant_id}"

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority_url,
            client_credential=self.client_secret,
        )
        result = app.acquire_token_for_client(scopes=self.scope)
        print("Printing access token:",result['access_token'])

        if "access_token" in result:
            return result["access_token"]
        else:
            raise Exception("Could not obtain access token")

    def get_site_id(self, access_token):
        url = f"https://graph.microsoft.com/v1.0/sites/{self.hostname}:/sites/{self.site_name}"
        print("URL PRINT:",url)
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()["id"]

    def get_drive_id(self, access_token, site_id):
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        drives = response.json()["value"]
        for drive in drives:
            if drive["name"] == "Documents":
                return drive["id"]

        raise Exception("Could not find the Documents drive")

    def read_file(self, access_token, site_id, drive_id, file_path, file_name):
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{file_path}:/content"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        if file_name.split(".")[-1] == "xlsx":
            print(f"Reading excel file... {file_name}")
            df = pd.read_excel(BytesIO(response.content),sheet_name="Sheet1")
            # print("Data frame :",df.columns)
            
        elif file_name.split(".")[-1] == "csv":
            print(f"Reading csv file... {file_name}")
            df = pd.read_csv(BytesIO(response.content))
        else:
            print(
                f"Issue occurred in file reading. The data might not be in .xlsx format. File name is {file_name}"
            )
            return None
        

        # with open(file_name, 'wb') as file:
        #     file.write(response.content)

        print(f"File read successfully to dataframe")
        return df

    def upload_file(
        self, access_token, site_id, drive_id, file_path_in_sharepoint, local_file_path
    ):
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{file_path_in_sharepoint}:/content"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/octet-stream",
        }

        with open(local_file_path, "rb") as file:
            file_data = file.read()

        response = requests.put(url, headers=headers, data=file_data)
        response.raise_for_status()

        print(f"File uploaded successfully to {file_path_in_sharepoint}")

    def get_folder_id(self, access_token, site_id, drive_id, folder_path):
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{folder_path}"

        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()["id"]



    def list_files_in_folder_with_dates(
        self, access_token, site_id, drive_id, folder_id
    ):

        print("date filter for files")
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{folder_id}/children"

        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        dates = []

        files = response.json().get("value", [])
        # print("+++++++++++++++++++++++++++++++++++++++++++++++")
        # print("Files of list_files_in_folder_with_dates:",files)
        client_files = []
        
        for file in files:
            print("list of all files present in sharepoint with modified time")
            time.sleep(3)
            print((file.get("lastModifiedDateTime")),file.get("name"))
        
        latest_file = max(files, key=lambda file: file.get("lastModifiedDateTime"))
        client_files.append(latest_file.get("name"))
        print(client_files)
        
        return client_files

    def delete_file(self, file_name):
        pass

    def upload_pdf_file(
        self, access_token, site_id, drive_id, file_path_in_sharepoint, pdf_data
    ):
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{file_path_in_sharepoint}:/content"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/pdf",
        }

        response = requests.put(url, headers=headers, data=pdf_data)
        response.raise_for_status()

        print(f"PDF uploaded successfully to {file_path_in_sharepoint}")

    
    def get_ids(self):
        print("Generating Access Token...")
        access_token = self.get_access_token()

        print("Getting site id...")
        site_id = self.get_site_id(access_token)

        print("Getting drive id...")
        drive_id = self.get_drive_id(access_token, site_id)

        print("Getting folder id...")
        folder_id = self.get_folder_id(
            access_token, site_id, drive_id, self.folder_path
        )

        return access_token, site_id, drive_id, folder_id
    
    
    def extract_instagram_shortcode(self, url):
        try:
            match = re.search(r"instagram\.com/(?:reel|p|tv)/([^/]+)/", url)
            return match.group(1) if match else None
        except Exception as e:
            print(f"Error extracting shortcode: {e}")
            return None

    def fetch_instagram_data(self, shortcode,url):
        try:
            L = instaloader.Instaloader(max_connection_attempts=1)
            post = instaloader.Post.from_shortcode(L.context, shortcode)
            data = {
                "Total Reach": post.owner_profile.followers,
                "Total Views": post.video_play_count if post.is_video else 0,
                "Total Likes": post.likes,
                "Total Comments": post.comments,
            }
            if data["Total Views"] >= THRESHOLD:
                post_message_to_teams(f"Instagram post {url} reached {data['Total Views']} views!")
            return data
        except Exception as e:
            print(f"Error fetching Instagram data: {e}")
            return {"Total Reach": "", "Total Views": "", "Total Likes": "", "Total Comments": ""}

    def fetch_youtube_data(self, url):
        try:
            ydl_opts = {"quiet": True, "no_warnings": True, "simulate": True, "geo_bypass": True}
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            data = {
                "Total Reach": info.get("view_count", ""),
                "Total Views": info.get("view_count", ""),
                "Total Likes": info.get("like_count", ""),
                "Total Comments": info.get("comment_count", ""),
            }
            if data["Total Views"] and int(data["Total Views"]) >= THRESHOLD:
                post_message_to_teams(f"YouTube video {url} reached {data['Total Views']} views!")
            return data
        except Exception as e:
            print(f"Error fetching YouTube data: {e}")
            return {"Total Reach": "", "Total Views": "", "Total Likes": "", "Total Comments": ""}

    # def process_batch(self, batch_df):
    #     results = []
    #     for _, row in batch_df.iterrows():
    #         link = row.get("Platform/ Go Live Link", "")
    #         influencer_name = row.get("Influencer Name", "")
    #         engagement_data = {"Total Reach": "", "Total Views": "", "Total Likes": "", "Total Comments": ""}
    #         try:
    #             if isinstance(link, str) and link:
    #                 if "instagram.com" in link:
    #                     shortcode = self.extract_instagram_shortcode(link)
    #                     engagement_data = self.fetch_instagram_data(shortcode,link) if shortcode else engagement_data
    #                 elif "youtube.com" in link or "youtu.be" in link:
    #                     engagement_data = self.fetch_youtube_data(link)
    #         except Exception as e:
    #             print(f"Error processing link {link}: {e}")
    #         results.append({"Influencer Name": influencer_name, "Platform/ Go Live Link": link, **engagement_data})
    #     return pd.DataFrame(results)
    
    def process_batch(self, batch_df):
        processed_rows = []  # Store only successfully processed rows

        for _, row in batch_df.iterrows():
            link = row.get("Platform/ Go Live Link", "")
            try:
                if isinstance(link, str) and link:
                    if "instagram.com" in link:
                        shortcode = self.extract_instagram_shortcode(link)
                        engagement_data = self.fetch_instagram_data(shortcode, link) if shortcode else {}
                    elif "youtube.com" in link or "youtu.be" in link:
                        engagement_data = self.fetch_youtube_data(link)
                    else:
                        engagement_data = {}

                    # Ensure we only keep rows where data was fetched
                    if engagement_data:
                        row["Views"] = engagement_data.get("Total Views", row.get("Views", 0))
                        row["Likes"] = engagement_data.get("Total Likes", row.get("Likes", 0))
                        row["Comments"] = engagement_data.get("Total Comments", row.get("Comments", 0))

                        processed_rows.append(row)  # Add only valid processed data
            
            except Exception as e:
                print(f"Error processing link {link}: {e}")

        # Create a new DataFrame with only processed rows (removes null/empty rows)
        return pd.DataFrame(processed_rows)

    
    
    def upload_dataframe_to_sharepoint(self,access_token, site_id, drive_id, folder_path, file_name, data_df):
        """
        Uploads a Pandas DataFrame as an Excel file to SharePoint using Microsoft Graph API.
        
        :param access_token: OAuth token for authentication
        :param site_id: SharePoint site ID
        :param drive_id: SharePoint drive ID
        :param folder_path: Path inside SharePoint where the file will be uploaded
        :param file_name: Name of the file to be uploaded
        :param data_df: Pandas DataFrame to be uploaded
        """
        
        output = BytesIO()
        data_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)  # Reset buffer position

       
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }

        # Upload the file
        response = requests.put(url, headers=headers, data=output)

        # Check for errors
        if response.status_code in [200, 201]:
            print(f"✅ File '{file_name}' successfully uploaded to SharePoint!")
        else:
            print(f"❌ Upload failed: {response.status_code}, {response.text}")

        return response.json()

    def process(self):
        try:
            # date = self.blob.connect_blob("download_blob_data", date=True)[0]["date"]
            # print("Final Date: " + str(date))
            
            date = "2024-12-23 09:24:39+00:00"

            # Adding date for subject
            date_today = datetime.now()
            date_today = date_today.strftime("%Y-%m-%d")

            if len(date):

                access_token, site_id, drive_id, folder_id = self.get_ids()
                files = self.list_files_in_folder_with_dates(
                    access_token, site_id, drive_id, folder_id
                )
                # print("files list:",files)
                print(("Files list: ", files))
                print("""%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%""")
                # print(("latest date:", latest_date))

                all_data_df = []  # Initialize an empty list to hold filtered DataFrames
                

                for file in files:
                    
                    
                    try:
                        print("checking connection!!!!!!!!!!!")                                  #akash
                        print(site_id)
                        print("++++++++++++++++")
                        time.sleep(10)
                        print(drive_id)
                        
                        # self.sql_server.connect_pyodbc()
                        data_df = self.read_file(
                            access_token,
                            site_id,
                            drive_id,
                            self.folder_path + file,
                            file,
                        )
                        
                        print("Data df :\n",data_df)
                        print("+++++++++++++++++++")
                        print("data_df shape:",data_df.columns)
                        print("data_df shape:",data_df.shape)
                        
                        if data_df is None:
                            continue

                        
                        if not set(["Platform/ Go Live Link"]).issubset(set(data_df.columns)):
                            print(f"Required columns missing in {file}. Skipping.")
                            continue
                        
                

                        processed_df = self.process_batch(data_df)

                        # Ensure only matching links get updated
                        columns_to_update = ["Views", "Likes", "Comments"]
                        processed_mapping = processed_df.set_index("Platform/ Go Live Link")[columns_to_update]

                        for col in columns_to_update:
                            data_df[col] = data_df["Platform/ Go Live Link"].map(processed_mapping[col])

                        # Convert columns to numeric (handle strings or incorrect types)
                        data_df[columns_to_update] = data_df[columns_to_update].apply(pd.to_numeric, errors="coerce")

                        # ❗ Drop rows where any of the columns are NaN or zero
                        data_df = data_df.replace(0, np.nan).dropna(subset=columns_to_update)

                        print(data_df.head(5))

                        
                        sharepoint_folder = "/Results_Campaign_Tracking/"
                        sharepoint_file_name = f"{file}"

                        
                        self.upload_dataframe_to_sharepoint(
                            access_token, site_id, drive_id, sharepoint_folder, sharepoint_file_name, data_df
                        )

                        

                    except Exception as e:
                        print(f"Error processing {file}: {str(e)}")
                        continue


        except Exception as e:
            
            print(f"{file} is not a proper sentiment analysis file")
            # continue
            #raise CustomException(e, sys)

        # self.blob_temp.delete_all_blobs_in_container()

        # # rewrite latest date
        # self.blob.connect_blob("upload", data=[{"date": latest_date}], date=True)
        # self.sql_server.close_connection()
        print("Processed files...")
        
        
    
    
