import pandas as pd
import os
import subprocess
import smtplib
import json
import zipfile
from io import BytesIO

my_email = os.environ.get("MY_EMAIL")
password = os.environ.get("EMAIL_PASSWORD")

pxstat_codes_dict = {'naq04': ['Statistic Label', 'Quarter', 'Sector', 'VALUE'],
                     'nqi01': ['Statistic Label', 'Quarter', 'Sectors', 'VALUE'],
                     'gfq10': ['Statistic Label', 'Quarter', 'State', 'VALUE'],
                     'gfq12': ['Statistic Label', 'Quarter', 'State', 'VALUE'],
                     'isq04': ['Statistic Label', 'Quarter', 'Ireland', 'VALUE'],
                     'bpq15': ['Statistic Label', 'Quarter', 'Sub Head', 'VALUE'],
                     'cpm18': ['Statistic Label', 'Month', 'Detailed Sub Indices', 'VALUE'],
                     'tsm01': ['Statistic Label', 'Month', 'State', 'VALUE'],
                     'wpm24': ['Statistic Label', 'Month', 'Industry Sector NACE Rev 2', 'VALUE'],
                     'rsm08': ['Statistic Label', 'Month', 'NACE Group', 'VALUE'],
                     'ndq01': ['STATISTIC Label', 'Quarter', 'Type of House', 'VALUE'],
                     'msi02': ['Statistic Label', 'Month', 'NACE Rev 2 Sector', 'VALUE'],
                     'tfq01': ['Statistic Label', 'Quarter', 'Business of Owner', 'VALUE'],
                     'gfq01': ['Statistic Label', 'Quarter', 'Item', 'VALUE'],
                     'gfq02': ['Statistic Label', 'Quarter', 'Item', 'VALUE'],
                     'tsm09': ['Statistic Label', 'Month', 'Commodity Group', 'Area', 'VALUE'],
                     'hpm09': ['Statistic Label', 'Month', 'Type of Residential Property', 'VALUE'],
                     'qlf03': ['Statistic Label', 'Quarter', 'Sex', 'NACE Rev 2 Economic Sector', 'VALUE'],
                     'mum01': ['Statistic Label', 'Month', 'Age Group', 'Sex', 'VALUE'],
                     'lrm13': ['Statistic Label', 'Month', 'Sex', 'Age Group', 'Last Held Occupation', 'VALUE'],
                     'ehq03': ['Statistic Label', 'Quarter', 'Economic Sector NACE Rev 2', 'Type of Employee', 'VALUE'],
                     'ndq06': ['STATISTIC Label', 'Quarter', 'Type of House', 'Local Authority', 'VALUE'],
                     'tfq02': ['Statistic Label', 'Quarter', 'NST Group', 'VALUE'],
                     'tem01': ['STATISTIC Label', 'Month', 'Taxation Class', 'VALUE'],
                     'naq05': ['Statistic Label', 'Quarter', 'Sector', 'VALUE'],
                     'na001': ['Statistic Label', 'Year', 'Item', 'VALUE'],
                     'na002': ['Statistic Label', 'Year', 'Item', 'VALUE'],
                     'isq01': ['STATISTIC Label', 'Quarter', 'Current Account', 'Institutional Sector', 'Uses and Resources', 'VALUE'],
                     'isq03': ['Statistic Label', 'Quarter', 'Current Account', 'Institutional Sector', 'Accounting Entry', 'VALUE'],
                     'qlf18': ['Statistic Label', 'Quarter', 'Age Group', 'Sex', 'VALUE'],
                     'naq02': ['Statistic Label', 'Quarter', 'Sector', 'VALUE'],
                     'naq06': ['Statistic Label', 'Quarter', 'Sector', 'VALUE'],
                     'naq07': ['Statistic Label', 'Quarter', 'NACE Rev. 2 Sector', 'VALUE'],
                     'naq08': ['Statistic Label', 'Quarter', 'NACE Rev. 2 Sector', 'VALUE']}

new_dict = {}

for table, labels in pxstat_codes_dict.items():
    data = pd.read_csv(f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{table}/CSV/1.0/en")
    try:
        data = data[labels]
        if 'Statistic Label' in data.columns:
            data.rename(columns = {'Statistic Label': 'Statistic', 'VALUE': 'value'}, inplace = True)
        elif 'STATISTIC Label' in data.columns:
            data.rename(columns = {'STATISTIC Label': 'Statistic', 'VALUE': 'value'}, inplace = True)
        data.columns = [col.replace(' ', '.') for col in data.columns]
        new_dict[table] = data
        data.to_csv(f'/home/flanneryj/econ_dash/{table}.csv', index = False)
    except Exception as e:
        print(f"An error occured while processing table {table}: {e}")

        with smtplib.SMTP("smtp.gmail.com", port = 587) as connection:
            connection.starttls()
            connection.login(user = my_email, password = password)
            connection.sendmail(
                from_addr = my_email,
                to_addrs = 'justin.flannery@cso.ie',
                msg = f"Subject:econ_dash_data error\n\n An error occured while processing table {table}: {e}")

        continue




# Output a combined parquet file

#all_columns = set()
#for df in new_dict.values():
#    all_columns.update(df.columns)

#combined_df = pd.concat([df.reindex(columns=all_columns) for df in new_dict.values()], ignore_index = True)
#combined_df['Table'] = [table for table, df in new_dict.items() for _ in range(len(df))]
#combined_df.to_csv('/home/flanneryj/econ_dash/combined_df.csv', index = False)
#combined_df.to_parquet('/home/flanneryj/econ_dash/combined_df.parquet')

# Check whether there are any changes to the table strings
final_dict = {}

for table, labels in new_dict.items():
    string_columns = labels.select_dtypes(include = ['object'])
    string_columns = string_columns.loc[:, ~(string_columns.columns.isin(['Month', 'Quarter', 'value', 'Value']))]
    tables_string_dict = {}
    for column in string_columns:
        tables_string_dict[column] = string_columns[column].unique().tolist()
    final_dict[table] = tables_string_dict

#with open('/home/flanneryj/econ_dash/econ_dash_dict.json', 'w') as json_file:
#    json.dump(final_dict, json_file)

with open('/home/flanneryj/econ_dash/econ_dash_dict.json', 'r') as json_file:
    old_dict = json.load(json_file)

def compare_dictionaries(dict1, dict2):
    if set(dict1.keys()) != set(dict2.keys()):
        return False, "Different tables"
    for table in dict1:
        if set(dict1[table].keys()) != set(dict2[table].keys()):
            return False, f"Different columns in table {table}"

        for column in dict1[table]:
            if set(dict1[table][column]) != set(dict2[table][column]):
                return False, f"Different values in column {column} of table {table}"

    return True, "Dictionaries are the same"

check_dicts = compare_dictionaries(final_dict, old_dict)
print(check_dicts)

if check_dicts[0] == False:
    with smtplib.SMTP("smtp.gmail.com", port = 587) as connection:
        connection.starttls()
        connection.login(user = my_email, password = password)
        connection.sendmail(
            from_addr = my_email,
            to_addrs = 'justin.flannery@cso.ie',
            msg = f"Subject:econ_dash_data changes\n\n {check_dicts[1]}")


four_col_tables = ('naq04', 'gfq10', 'gfq12', 'isq04', 'bpq15', 'cpm18', 'tsm01', 'wpm24', 'rsm08', 'ndq01', 'msi02', 'tfq01',
                   'gfq01', 'gfq02', 'hpm09', 'tfq02', 'tem01', 'naq05', 'na001', 'na002', 'naq02', 'naq06', 'naq07', 'naq08')


five_col_tables = ('tsm09', 'qlf03', 'mum01', 'ehq03', 'ndq06', 'qlf18')

six_col_tables = ('lrm13', 'isq01', 'isq03')

for label, data in new_dict.items():
    if label in four_col_tables:
        data.columns = ['A', 'B', 'C', 'D']
    elif label in five_col_tables:
        data.columns = ['A', 'B', 'C', 'D', 'E']
    elif label in six_col_tables:
        data.columns = ['A', 'B', 'C', 'D', 'E', 'F']

four_col_df = pd.concat([data for label, data in new_dict.items() if label in four_col_tables], ignore_index = True)
five_col_df = pd.concat([data for label, data in new_dict.items() if label in five_col_tables], ignore_index = True)
six_col_df = pd.concat([data for label, data in new_dict.items() if label in six_col_tables], ignore_index = True)

four_col_df['Table'] = [table for table, df in new_dict.items() for _ in range(len(df)) if table in four_col_tables]
five_col_df['Table'] = [table for table, df in new_dict.items() for _ in range(len(df)) if table in five_col_tables]
six_col_df['Table'] = [table for table, df in new_dict.items() for _ in range(len(df)) if table in six_col_tables]

four_col_df.to_csv('/home/flanneryj/econ_dash/four_col_df.csv', index = False)
five_col_df.to_csv('/home/flanneryj/econ_dash/five_col_df.csv', index = False)
six_col_df.to_csv('/home/flanneryj/econ_dash/six_col_df.csv', index = False)

dataframe = {
    'four_col_df.csv':four_col_df,
    'five_col_df.csv':five_col_df,
    'six_col_df.csv': six_col_df
}

zip_filename = '/home/flanneryj/econ_dash/dash_df.zip'

with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
    for filename, df in dataframe.items():
        with zf.open(filename, 'w') as buffer:
            df.to_csv(buffer, index = False)

def git_push():
    project_path = '/home/flanneryj/econ_dash'
    os.chdir(project_path)

    try:

        github_token = os.environ.get("GITHUB_TOKEN")
        if not github_token:
            raise Exception("GitHub token not found in environment variables.")
        username = 'flanneryjcso'
        repository = 'econ_dash_data'
        url = f'https://{username}:{github_token}@github.com/{username}/{repository}.git'


        subprocess.run(['git', 'remote', 'set-url', 'origin', url], check=True)
        subprocess.run(['git', 'add', '.'], check=True)
        subprocess.run(['git', 'commit', '-m', 'Automated commit of CSV files'], check=True)
        subprocess.run(['git', 'push', 'origin', 'main'], check=True)
        print("Successfully pushed to GitHub.")

    except subprocess.CalledProcessError as e:

        print("Failed to push to GitHub:", e)

git_push()
