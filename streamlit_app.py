import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import tempfile
import json
import pytz

# Define the scope
SCOPE = ['https://www.googleapis.com/auth/drive']

# Load credentials from Streamlit secrets
credentials_path = st.secrets["google"]["GOOGLE_CREDENTIALS"]
creds_dict = json.loads(credentials_path)

# Authorize using the credentials
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
CLIENT = gspread.authorize(creds)

# Use your folder ID
folder_id = "1HifJfkEqrqvoRz9uPXkOAiF-Wy9VZUfr"

def upload_to_drive(uploaded_file_path, file_name, folder_id):
    try:
        drive_service = build('drive', 'v3', credentials=creds)
        
        # Search for an existing file with the same name in the specified folder
        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        existing_files = results.get('files', [])

        if existing_files:
            # If the file exists, update it
            file_id = existing_files[0]['id']
            media = MediaFileUpload(uploaded_file_path, mimetype='text/csv')
            updated_file = drive_service.files().update(fileId=file_id, media_body=media).execute()
            return updated_file.get('id'), f"https://drive.google.com/file/d/{updated_file.get('id')}/view"
        else:
            # If the file doesn't exist, create a new one
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            media = MediaFileUpload(uploaded_file_path, mimetype='text/csv')
            new_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
            return new_file.get('id'), f"https://drive.google.com/file/d/{new_file.get('id')}/view"
    except Exception as e:
        st.error(f"Failed to upload file: {e}")
        print(f"Error: {e}")
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
        
# Function to process uploaded files
def process_files(member_outreach_file, event_debrief_file, submitted_file, approved_file):
    # Load the uploaded files into DataFrames
    outreach_dfs = []
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

    schools_on_events = [
        ('UT Arlington', 'UT ARLINGTON'),
        ('Santa Clara', 'SANTA CLARA'),
        ('UCLA', 'UCLA'),
        ('LMU', 'LMU'),
        ('Pepperdine', 'PEPPERDINE'),
        ('UC Irvine', 'UC IRVINE'),
        ('UC San Diego', 'UC SAN DIEGO'),
        ("Saint Mary's", "SAINT MARY'S"),
        ('UC Davis', 'UC DAVIS'),
    ]

    growth_officer_mapping = {
        'Ileana': 'Ileana Heredia',
        'ileana': 'Ileana Heredia',
        'BK': 'Brian Kahmar',
        'JR': 'Julia Racioppo',
        'Jordan': 'Jordan Richied',
        'VN': 'Veronica Nims',
        'vn': 'Veronica Nims',
        'Dom': 'Domenic Noto',
        'Megan': 'Megan Sterling',
        'Megan ': 'Megan Sterling',
        'Jordan/Megan': 'Megan Sterling',
        'Veronica': 'Veronica Nims',
        'SB': 'Sheena Barlow',
        'Julio': 'Julio Macias',
        'Mo': 'Monisha Donaldson',
    }

    event_to_outreach_mapping = {event: outreach for event, outreach in schools_on_events}

    # Process each school sheet
    for sheet_name, school in schools:
        outreach_df = pd.read_excel(member_outreach_file, sheet_name=sheet_name)
        event_df = pd.read_excel(event_debrief_file,skiprows=1)

        outreach_df.columns = [f'outreach_{col}' for col in outreach_df.columns]
        outreach_df['outreach_Growth Officer'] = outreach_df['outreach_Growth Officer'].replace(growth_officer_mapping)
        outreach_df['outreach_school_name'] = school

        mapped_school_name = next((event for event, outreach in schools_on_events if outreach == school), None)
        events_df = event_df[event_df['Select Your School'].str.strip().str.upper() == mapped_school_name.upper()]

        outreach_df['outreach_Date'] = pd.to_datetime(outreach_df['outreach_Date'], errors='coerce')
        events_df['Date of the Event'] = pd.to_datetime(events_df['Date of the Event'], errors='coerce')

        outreach_df = outreach_df.dropna(subset=['outreach_Date'])
        events_df = events_df.dropna(subset=['Date of the Event'])

        if 'outreach_event_name' not in outreach_df.columns:
            outreach_df['outreach_event_name'] = None

        event_columns = [
            'Email', 'Request type?', 'In-Contract or Out-of-Contract?',
            'Host or Department', 'Date of the Event', 'Location', 'Audience'
        ]
        renamed_event_columns = {col: f'event_{col}' for col in event_columns}

        for i, outreach_row in outreach_df.iterrows():
            closest_event = None
            closest_diff = timedelta(days=11)

            for _, event_row in events_df.iterrows():
                if (
                    outreach_row['outreach_Date'] >= event_row['Date of the Event'] and
                    outreach_row['outreach_Date'] <= event_row['Date of the Event'] + timedelta(days=10) and
                    outreach_row['outreach_school_name'].strip().upper() == event_row['Select Your School'].strip().upper()
                ):
                    date_diff = outreach_row['outreach_Date'] - event_row['Date of the Event']
                    if date_diff < closest_diff:
                        closest_diff = date_diff
                        closest_event = event_row

            if closest_event is not None:
                outreach_df.at[i, 'outreach_event_name'] = closest_event['Event Name']
                for col in event_columns:
                    outreach_df.at[i, renamed_event_columns[col]] = closest_event[col]

                if pd.isna(outreach_df.at[i, 'outreach_Growth Officer']):
                    outreach_df.at[i, 'outreach_Growth Officer'] = closest_event['Name']

        outreach_df = outreach_df.dropna(subset=['outreach_Growth Officer'])
        outreach_df = outreach_df.dropna(subset=['outreach_event_name'])

        outreach_dfs.append(outreach_df)

    final_df = pd.concat(outreach_dfs, ignore_index=True)
    final_df = final_df.dropna(subset=['outreach_Date'])

    submitted_df = pd.read_excel(submitted_file)
    approved_df = pd.read_excel(approved_file)
    approved_df['status'] = 'Approved'
    submitted_df['autoApproved'] = submitted_df['status'].apply(
        lambda x: 'Yes' if x == 'Auto Approved' else ('No' if x == 'Approved' else '')
    )
    submitted_df['status'] = submitted_df['status'].replace('Auto Approved', 'Approved')
    combined_data = pd.concat([submitted_df, approved_df], ignore_index=True)

    def update_from_approved(row):
        if row['status'] == 'Approved' and row['memberName'] in approved_df['memberName'].values:
            match = approved_df.loc[approved_df['memberName'] == row['memberName']]
            if not match.empty:
                row['autoApproved'] = match['autoApproved'].values[0]
                row['funded'] = match['funded'].values[0] if 'funded' in match.columns else None
                row['bankingAccessed'] = match['bankingAccessed'].values[0] if 'bankingAccessed' in match.columns else None
                row['directDepositAttempted'] = match['directDepositAttempted'].values[0] if 'directDepositAttempted' in match.columns else None
        return row

    combined_data = combined_data.apply(update_from_approved, axis=1)
    cleaned_data = combined_data.drop_duplicates(
        subset=['memberName', 'applicationStartDate', 'applicationSubmittedDate', 'applicationApprovalDate', 'status']
    )
     # Add creation of the 'School Affiliation' column
    cleaned_data['Affiliation'] = cleaned_data['What is your affiliation?'].fillna('') + ' ' + \
                                         cleaned_data['What organization are you affiliated with?'].fillna('') + ' ' + \
                                         cleaned_data['What university do you attend?'].fillna('') + ' ' + \
                                         cleaned_data['Who is your employer?'].fillna('')

    # Remove extra spaces and trim the new 'School Affiliation' column
    cleaned_data['Affiliation'] = cleaned_data['Affiliation'].str.strip()

    cleaned_data = cleaned_data.rename(columns={col: f'submitted_{col}' for col in cleaned_data.columns})

    final_df_cleaned = pd.merge(final_df, cleaned_data, left_on='outreach_Name', right_on='submitted_memberName', how='left')
    
    # Save the final cleaned DataFrame to a temporary CSV file for download
    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as temp_csv:
        final_df_cleaned.to_csv(temp_csv.name, index=False)
        temp_csv_path = temp_csv.name

    return final_df_cleaned, temp_csv_path
    
    

# Streamlit app UI
def main():
    st.title("File Upload and Processing")
    st.write("Please submit the following files and make sure they are in the correct format: CSV and/or XLSX only.")
     # File upload
    member_outreach_file = st.file_uploader("Upload Member Outreach File (CSV/XLSX)", type=["csv", "xlsx"])
    event_debrief_file = st.file_uploader("Upload Event Debrief File (CSV/XLSX)", type=["csv", "xlsx"])
    submitted_file = st.file_uploader("Upload Submitted File (CSV/XLSX)", type=["csv", "xlsx"])
    approved_file = st.file_uploader("Upload Approved File (CSV/XLSX)", type=["csv", "xlsx"])
    
    if member_outreach_file and event_debrief_file and submitted_file and approved_file:
        if st.button("Clean Data"):
            result_df, temp_file_path = process_files(member_outreach_file, event_debrief_file, submitted_file, approved_file)
            st.success("Data cleaned successfully!")
            st.write(result_df)

             # Outreach Name Count Summary
            st.header("Outreach Name Count Summary")
            outreach_name_counts = result_df['outreach_Name'].value_counts()
            only_once = (outreach_name_counts == 1).sum()
            only_twice = (outreach_name_counts == 2).sum()
            more_than_twice = (outreach_name_counts > 2).sum()

            st.write(f"Number of names found only once: {only_once}")
            st.write(f"Number of names found only twice: {only_twice}")
            st.write(f"Number of names found more than twice: {more_than_twice}")
            
            # Growth Officer Report
            st.header("Growth Officer Report")
            growth_officer_counts = result_df.groupby('outreach_Growth Officer')['outreach_Name'].count()
            st.write("Number of names assigned to each Growth Officer:")
            st.dataframe(growth_officer_counts.rename("Assigned Names Count").reset_index())

            # Growth Officer by Event Report
            st.header("Growth Officer by Event Report")
            growth_officer_by_event = result_df.groupby('outreach_event_name')['outreach_Growth Officer'].nunique()
            st.write("Number of distinct Growth Officers assigned to each Event:")
            st.dataframe(growth_officer_by_event.rename("Distinct Growth Officers Count").reset_index())
            
            # Convert the current timestamp to PST
            now_utc = datetime.now(pytz.utc)
            now_pacific = now_utc.astimezone(pytz.timezone('US/Pacific'))
            formatted_pacific_time = now_pacific.strftime('%Y%m%d_%H%M%S')
            
            # Option to download the result as CSV
            st.header("Download Processed Data")
            st.download_button(
                label="Download CSV",
                data=open(temp_file_path, 'rb').read(),
                file_name=f"UCU_{formatted_pacific_time}.csv",
                mime="text/csv"
            )
            
            # Upload to Google Drive with PST timestamp
            st.header("Upload to Google Drive")
            
            # File with a timestamp in PST
            file_id, file_link = upload_to_drive(
                temp_file_path, 
                f"UCU_{formatted_pacific_time}.csv", 
                folder_id
            )
            if file_id:
                st.write(f"File uploaded to Google Drive with timestamp: [Link to File](https://drive.google.com/file/d/{file_id}/view)")
            
            # File with a fixed name
            file_id_2, file_link_2 = upload_to_drive(
                temp_file_path, 
                "UCU_Dashboard_linked.csv", 
                folder_id
            )
            if file_id_2:
                st.write(f"File also saved as 'UCU_Dashboard_linked.csv': [Link to File](https://drive.google.com/file/d/{file_id_2}/view)")
           

if __name__ == "__main__":
    main()

