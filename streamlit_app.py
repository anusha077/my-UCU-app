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
        # Schools mapping
        schools = [
            ('UTA', 'UT ARLINGTON'),
            ('SCU', 'SANTA CLARA'),
            ('UCLA', 'UCLA'),
            ('LMU', 'LMU'),
            ('Pepperdine', 'PEPPERDINE'),
            ('Irvine', 'UC IRVINE'),
            ('San Diego', 'UC SAN DIEGO'),
            ('SMC', "SAINT MARY'S"),
            ('Davis', 'UC DAVIS'),
        ]

        outreach_dfs = []
        growth_officer_mapping = {
            'Ileana': 'Ileana Heredia',
            'ileana': 'Ileana Heredia',
            'BK': 'Brian Kahmar',
            'JR': 'Julia Racioppo',
            'Jordan': 'Jordan Richied',
            'VN': 'Veronica Nims',
            'Dom': 'Domenic Noto',
            'Megan': 'Megan Sterling',
            'Veronica': 'Veronica Nims',
            'SB': 'Sheena Barlow',
            'Julio': 'Julio Macias',
            'Mo': 'Monisha Donaldson',
        }

        # Process outreach sheets
        for sheet_name, school in schools:
            try:
                outreach_df = pd.read_excel(member_outreach_file, sheet_name=sheet_name)
                outreach_df.columns = [f'outreach_{col}' for col in outreach_df.columns]
            except Exception as e:
                st.error(f"Error processing sheet '{sheet_name}': {e}")
                continue

            required_columns = ['outreach_Date', 'outreach_Growth Officer']
            if any(col not in outreach_df.columns for col in required_columns):
                st.error(f"Missing columns in outreach data for {school}.")
                continue

            outreach_df['outreach_school_name'] = school
            outreach_df['outreach_Growth Officer'] = outreach_df['outreach_Growth Officer'].replace(growth_officer_mapping)

            # Process dates
            outreach_df['outreach_Date'] = pd.to_datetime(outreach_df['outreach_Date'], errors='coerce')
            outreach_df = outreach_df.dropna(subset=['outreach_Date'])

            outreach_dfs.append(outreach_df)

        final_outreach_df = pd.concat(outreach_dfs, ignore_index=True)

        # Load and process event debrief data
        event_df = pd.read_excel(event_debrief_file, skiprows=1)
        event_df['Date of the Event'] = pd.to_datetime(event_df['Date of the Event'], errors='coerce')
        event_df = event_df.dropna(subset=['Date of the Event'])

        # Match events to outreach data
        for i, outreach_row in final_outreach_df.iterrows():
            closest_event = None
            closest_diff = timedelta(days=11)
            for _, event_row in event_df.iterrows():
                if (
                    event_row['Date of the Event'] <= outreach_row['outreach_Date'] <= event_row['Date of the Event'] + timedelta(days=10) and
                    outreach_row['outreach_school_name'] == event_row.get('Select Your School', '')
                ):
                    date_diff = outreach_row['outreach_Date'] - event_row['Date of the Event']
                    if date_diff < closest_diff:
                        closest_diff = date_diff
                        closest_event = event_row

            if closest_event is not None:
                final_outreach_df.at[i, 'outreach_event_name'] = closest_event['Event Name']

        # Load and merge submitted and approved data
        submitted_df = pd.read_excel(submitted_file)
        approved_df = pd.read_excel(approved_file)

        approved_df['status'] = 'Approved'
        submitted_df['autoApproved'] = submitted_df['status'].apply(
            lambda x: 'Yes' if x == 'Auto Approved' else 'No'
        )
        submitted_df['status'] = submitted_df['status'].replace('Auto Approved', 'Approved')

        combined_data = pd.concat([submitted_df, approved_df], ignore_index=True)
        combined_data.drop_duplicates(
            subset=['memberName', 'applicationStartDate', 'applicationApprovalDate', 'status'], inplace=True
        )

        # Merge with outreach data
        final_df_cleaned = pd.merge(
            final_outreach_df, combined_data, left_on='outreach_Name', right_on='memberName', how='left'
        )

        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
            final_df_cleaned.to_csv(temp_csv.name, index=False)
            temp_csv_path = temp_csv.name

        return final_df_cleaned, temp_csv_path

    except Exception as e:
        st.error(f"An error occurred during file processing: {e}")
        return None, None

def main():
    st.title("File Upload and Processing")

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
