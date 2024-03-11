import pandas as pd
import gspread
from google.oauth2 import service_account
import json
import os


json_file_name = "GOOGLE_JSON.json"  # Specify the name of your JSON file in the current working directory

# run specific variables:

# Quiz spreadsheet
spread_sheet_url_1 = ''  # url of quiz sheet
sheet_num_1 = 0
used_headers_1 = ['Score', 'Your Group', 'Email']  # used headers from the sheet
# also update line 74

# Results spreadsheet
spread_sheet_url_2 = ''  # url of scoring sheet
sheet_num_2 = [f"Group {x}" for x in range(1, 5)]  # sheet names
used_col_2 = "B"  # col. letter read for emails
target_col = ""  # col. letter to write to

# Specify the name of your environment variable
google_json = "GOOGLE_JSON"

# Read the content of the JSON file
try:
    with open(json_file_name, "r") as file:
        json_content = file.read()
except FileNotFoundError:
    print(f"Error: JSON file '{json_file_name}' not found.")
    json_content = ""

# Set the environment variable
os.environ[google_json] = json.dumps(json_content)

# Print a confirmation message
print(f"Environment variable '{google_json}' set with the content of '{json_file_name}'.")

# read from first spreadsheet
# Import data and create data frame
service_account_info = json.loads(json_content)
credentials = service_account.Credentials.from_service_account_info(service_account_info)
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds_with_scope = credentials.with_scopes(scope)
client = gspread.authorize(creds_with_scope)
spreadsheet_1 = client.open_by_url(spread_sheet_url_1)
worksheet_1 = spreadsheet_1.get_worksheet(sheet_num_1)
records_data_1 = worksheet_1.get_all_records(expected_headers=used_headers_1)
records_df_1 = pd.DataFrame.from_dict([{header: record[header] for header in used_headers_1} for record in records_data_1])

# read from 2nd spreadsheet
# Import data and create data frame
service_account_info = json.loads(json_content)
credentials = service_account.Credentials.from_service_account_info(service_account_info)
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds_with_scope = credentials.with_scopes(scope)
client = gspread.authorize(creds_with_scope)
spreadsheet_2 = client.open_by_url(spread_sheet_url_2)

# create r2 df, with all sheets in it
records_df_2 = pd.DataFrame()
for worksheet_name in sheet_num_2:
    # Get the data from the current worksheet
    worksheet = spreadsheet_2.worksheet(worksheet_name)
    temp_df = pd.DataFrame(enumerate(worksheet.col_values(ord(used_col_2) - ord("A") + 1)[1:]))
    # Append the data to the merged DataFrame
    records_df_2 = pd.concat([records_df_2, temp_df], ignore_index=True)
records_df_2.columns = ["Index", "Email"]


# rename header for standard
records_df_1.rename(columns={"Score": "Grade"}, inplace=True)

# extract grade:
records_df_1["Grade"] = records_df_1["Grade"].apply(lambda x:  x.split("/")[0])

# clean Emails
records_df_1["Email"] = records_df_1["Email"].apply(lambda x: x.strip().lower())
records_df_2["Email"] = records_df_2["Email"].apply(str)
records_df_2["Email"] = records_df_2["Email"].apply(lambda x: x.strip().lower())

# create merged df
merged_df = pd.merge(records_df_1, records_df_2, on='Email', how='outer', indicator=True)

# Drop unmerged rows
failed = merged_df[merged_df['_merge'] == 'left_only']
merged_df = merged_df[merged_df['_merge'] == 'both']

# Drop the '_merge' column
merged_df = merged_df.drop(columns=['_merge'])
failed = failed.drop(columns=['_merge'])

# create csv of all bad emails
failed.to_csv(f"failed.csv", index=False)
print("Non-matching emails put in failed.csv")

# loop by group to split into sheets
for group_name in sheet_num_2:
    group_members = merged_df[merged_df["Your Group"] == group_name]
    worksheet = spreadsheet_2.worksheet(group_name)

    for trash_index1, row in group_members.iterrows():
        # write Grade to cell
        try:
            row_number = int(row["Index"] + 2)
            selected_row = merged_df.loc[(merged_df['Your Group'] == group_name) & (merged_df['Index'] == row_number - 2)]
            grade_value = selected_row["Grade"].iloc[-1]

            worksheet.update_cell(row_number, ord(target_col) - ord("A") + 1, grade_value)
            print(f'Updated cell in sheet {worksheet} column {target_col} for row {row_number} with value {grade_value}, its email is {row["Email"]}')
        except:
            print(f'Target value "{row["Email"]}" caused an issue')
            pass

