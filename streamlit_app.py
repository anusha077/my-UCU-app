import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import tempfile
import json

# Define the scope
SCOPE = ['https://www.googleapis.com/auth/drive']

# Load credentials from Streamlit secrets
try:
    credentials_path = st.secrets["google"]["GOOGLE_CREDENTIALS"]
    creds_dict = json.loads(credentials_path)
except KeyError:
    st.error("Google credentials are missing in the Streamlit secrets configuration.")
    st.stop()

# Authorize using the credentials
try:
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    CLIENT = gspread.authorize(creds)
except Exception as e:
    st.error(f"Failed to authenticate Google credentials: {e}")
    st.stop()

# Use your folder ID
folder_id = "1HifJfkEqrqvoRz9uPXkOAiF-Wy9VZUfr"

def upload_to_drive(uploaded_file_path, file_name, folder_id):
    try:
        drive_service = build('drive', 'v3', credentials=creds)
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        media = MediaFileUpload(uploaded_file_path, mimetype='text/csv')
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        return file.get('id'), file.get('webViewLink')
    except Exception as e:
        st.error(f"Failed to upload file: {e}")
        return None, None

def read_file(uploaded_file, file_label):
    try:
        if uploaded_file.name.endswith(".csv"):
            return pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(".xlsx"):
            # Try reading with openpyxl engine
            excel_data = pd.ExcelFile(uploaded_file, engine='openpyxl')
            if len(excel_data.sheet_names) == 0:
                raise ValueError(f"No worksheets found in the file '{file_label}'.")
            # Read the first sheet by index (index 0)
            return excel_data.parse(0)  # 0 refers to the first sheet
        else:
            raise ValueError(f"Unsupported file type for '{file_label}'. Please upload a CSV or Excel file.")
    except Exception as e:
        st.error(f"Error reading the file '{file_label}': {str(e)}")
        return None

def process_files(member_outreach_file, event_debrief_file, submitted_file, approved_file):
    try:
        # Processing logic remains unchanged
        pass
def main():
    st.title("UCU File Uploader")

    st.write("Please submit the following files and make sure they are in the correct format: CSV and/or XLSX only.")

    # File upload
    member_outreach_file = st.file_uploader("Upload Member Outreach File (CSV/XLSX)", type=["csv", "xlsx"])
    event_debrief_file = st.file_uploader("Upload Event Debrief File (CSV/XLSX)", type=["csv", "xlsx"])
    submitted_file = st.file_uploader("Upload Submitted File (CSV/XLSX)", type=["csv", "xlsx"])
    approved_file = st.file_uploader("Upload Approved File (CSV/XLSX)", type=["csv", "xlsx"])

    if member_outreach_file and event_debrief_file and submitted_file and approved_file:
        if st.button("Clean Data"):
            # Read uploaded files
            member_outreach_data = read_file(member_outreach_file, "Member Outreach File")
            event_debrief_data = read_file(event_debrief_file, "Event Debrief File")
            submitted_data = read_file(submitted_file, "Submitted File")
            approved_data = read_file(approved_file, "Approved File")

            # Validate that files were read correctly
            if any(data is None for data in [member_outreach_data, event_debrief_data, submitted_data, approved_data]):
                st.error("One or more files could not be read. Please check your uploads.")
                return

            # Process files
            result_df, temp_file_path = process_files(member_outreach_file, event_debrief_file, submitted_file, approved_file)

            if result_df is not None:
                st.success("Data cleaned successfully!")
                st.write(result_df)

                # Option to download the result as CSV
                st.header("Download Processed Data")
                st.download_button(
                    label="Download CSV",
                    data=open(temp_file_path, 'rb').read(),
                    file_name=f"UCU_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

                # Upload to Google Drive
                file_id, file_link = upload_to_drive(temp_file_path, f"UCU_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", folder_id)
                if file_id:
                    st.write(f"File uploaded to Google Drive: [Link to File](https://drive.google.com/file/d/{file_id}/view)")
            else:
                st.error("Data processing failed. Please check the uploaded files.")

if __name__ == "__main__":
    main()
