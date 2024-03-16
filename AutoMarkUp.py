import pandas as pd
import gspread
from google.oauth2 import service_account
import json
import os
from time import sleep


def read_google_json(google_json):  # Read the content of the JSON file
    try:
        with open(json_file_name, "r") as file:
            json_content = file.read()
    except FileNotFoundError:
        print(f"Error: JSON file '{json_file_name}' not found.")
        json_content = ""
    # Set the environment variable
    os.environ[google_json] = json.dumps(json_content)
    print(f"Environment variable '{google_json}' set with the content of '{json_file_name}'.")
    return json_content


def g_authorise(json_content):
    service_account_info = json.loads(json_content)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_with_scope = credentials.with_scopes(scope)
    func_client = gspread.authorize(creds_with_scope)
    return func_client


def import_spreadsheet(spread_sheet_url, sheet_num, used_headers, col_letter=None):
    spreadsheet = client.open_by_url(spread_sheet_url)
    if type(sheet_num) == int:
        worksheet = spreadsheet.get_worksheet(sheet_num)
        records_data = worksheet.get_all_records(expected_headers=used_headers)
        records_data_trimmed = [{key: record[key] for key in used_headers} for record in records_data]
        records_df = pd.DataFrame.from_dict(records_data_trimmed)
    elif type(sheet_num) == list:
        records_df = merge_sheets(spreadsheet, sheet_num, col_letter)
    return records_df


def merge_sheets(spreadsheet, worksheet_names, col_letter):
    records_df = pd.DataFrame()
    for worksheet_name in worksheet_names:
        # Get the data from the current worksheet
        worksheet = spreadsheet.worksheet(worksheet_name)
        temp_df = pd.DataFrame(enumerate(worksheet.col_values(ord(col_letter) - ord("A") + 1)[1:]))
        # Append the data to the merged DataFrame
        records_df = pd.concat([records_df, temp_df], ignore_index=True)
    return records_df


# rename header for standard
def rename_header(df, old_name, new_name="Grade"):
    df = df.rename(columns={old_name: new_name})
    return df


def extract_grade(df):
    df["Grade"] = df["Grade"].apply(lambda x:  x.split("/")[0])
    return df


def clean_emails(df):
    df["Email"] = df["Email"].apply(str)
    df["Email"] = df["Email"].apply(lambda x: x.strip().lower())
    return df


def merge_dfs(quiz_df, grades_df, merge_col):
    merged_df = pd.merge(quiz_df, grades_df, on=merge_col, how='outer', indicator=True)
    # Drop unmerged rows
    failed_to_merge = merged_df[merged_df['_merge'] == 'left_only']
    merged_df = merged_df[merged_df['_merge'] == 'both']
    # Drop the '_merge' column
    merged_df = merged_df.drop(columns=['_merge'])
    failed_to_merge = failed_to_merge.drop(columns=['_merge'])
    # create csv of all bad emails
    failed_to_merge.to_csv(f"failed_to_merge", index=False)
    print("Non-matching emails put in failed_to_merge.csv")
    return merged_df


# loop by group to split into sheets
def write_to_cloud(spread_sheet_df, spread_sheet_url, work_sheet_names, target_col ,sheet_indicator=None, sheet_indicator_col=None):
    if sheet_indicator_col is None:
        sheet_indicator_col = "Your Group"

    spread_sheet = client.open_by_url(spread_sheet_url)
    for sheet in work_sheet_names:
        sheet_rows = spread_sheet_df[spread_sheet_df[sheet_indicator_col] == sheet]
        worksheet = spread_sheet.worksheet(sheet)

        if sheet_indicator is None:
            sheet_indicator = sheet

        for trash_index, row in sheet_rows.iterrows():
            # write Grade to cell
            try:
                row_number = int(row["Index"] + 2)
                grade_value = row["Grade"]

                worksheet.update_cell(row_number, ord(target_col) - ord("A") + 1, grade_value)
                print(f'Updated cell in sheet {worksheet} column {target_col} for row {row_number} with value {grade_value}, its email is {row["Email"]}')
                sleep(.02)  # Avoid rate cap (~0.16)

            except Exception as e:
                print(f'Target value "{row["Email"]}" caused an issue {repr(e)}')


if __name__ == "__main__":
    json_file_name = "GOOGLE_JSON.json"  # Specify the name of your JSON file in the current working directory

    # run specific variables:

    # Quiz spreadsheet
    spread_sheet_url_1 = ''  # url of quiz sheet
    sheet_index_1 = 0
    used_headers_1 = ['النتيجة', 'Your Group', 'Email']  # used headers from the sheet
    # Results spreadsheet
    spread_sheet_url_2 = ''  # url of scoring sheet
    sheet_list_2 = [f"Group {x}" for x in range(1, 5)]  # sheet names
    used_col_letter = "B"  # col. letter read for emails
    used_col_head = ["Index", "Email"]
    target_col = "K"  # col. letter to write to

    merge_column = "Email"
    # Specify the name of your environment variable
    google_json_file = "GOOGLE_JSON"

    # Script:
    json_data = read_google_json(google_json_file)
    client = g_authorise(json_data)

    quiz_records_df = import_spreadsheet(spread_sheet_url_1, sheet_index_1, used_headers_1)
    quiz_records_df = rename_header(quiz_records_df, used_headers_1[0])
    quiz_records_df = clean_emails(quiz_records_df)
    quiz_records_df = extract_grade(quiz_records_df)

    grade_records_df = import_spreadsheet(spread_sheet_url_2, sheet_list_2, target_col, used_col_letter)
    grade_records_df.columns = used_col_head
    grade_records_df = clean_emails(grade_records_df)

    df_merged = merge_dfs(quiz_records_df, grade_records_df, merge_column)

    write_to_cloud(df_merged, spread_sheet_url_2, sheet_list_2, target_col)

