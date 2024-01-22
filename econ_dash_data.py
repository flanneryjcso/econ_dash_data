import pandas as pd
import os
import subprocess

pxstat_codes_dict = {'naq04': ['Statistic Label', 'Quarter', 'Sector', 'VALUE'],
                     'nqi01': ['Statistic Label', 'Quarter', 'Sectors', 'VALUE'],
                     'gfq10': ['Statistic Label', 'Quarter', 'State', 'VALUE'],
                     'gfq12': ['Statistic Label', 'Quarter', 'State', 'VALUE'],
                     'isq04': ['Statistic Label', 'Quarter', 'Ireland', 'VALUE'],
                     'bpq15': ['Statistic Label', 'Quarter', 'Sub Head', 'VALUE'],
                     'cpm16': ['Statistic Label', 'Month', 'Detailed Sub Indices', 'VALUE'],
                     'tsm01': ['Statistic Label', 'Month', 'State', 'VALUE'],
                     'wpm24': ['Statistic Label', 'Month', 'Industry Sector NACE Rev 2', 'VALUE'],
                     'rsm05': ['Statistic Label', 'Month', 'NACE Group', 'VALUE'],
                     'ndq07': ['STATISTIC Label', 'Quarter', 'Eircode Output', 'VALUE'],
                     'msi02': ['Statistic Label', 'Month', 'NACE Rev 2 Sector', 'VALUE'],
                     'tfq01': ['Statistic Label', 'Quarter', 'Business of Owner', 'VALUE'],
                     'gfq01': ['Statistic Label', 'Quarter', 'Item', 'VALUE'],
                     'gfq02': ['Statistic Label', 'Quarter', 'Item', 'VALUE'],
                     'tsm09': ['Statistic Label', 'Month', 'Commodity Group', 'Area', 'VALUE'],
                     'hpm09': ['Statistic Label', 'Month', 'Type of Residential Property', 'VALUE'],
                     'qlf03': ['Statistic Label', 'Quarter', 'Sex', 'NACE Rev 2 Economic Sector', 'VALUE'],
                     'mum01': ['Statistic Label', 'Month', 'Age Group', 'Sex', 'VALUE'],
                     'lrm13': ['STATISTIC Label', 'Month', 'Sex', 'Age Group', 'Last Held Occupation', 'VALUE'],
                     'ehq03': ['Statistic Label', 'Quarter', 'Economic Sector NACE Rev 2', 'Type of Employee', 'VALUE'],
                     'ndq06': ['STATISTIC Label', 'Quarter', 'Type of House', 'Local Authority', 'VALUE'],
                     'tfq02': ['Statistic Label', 'Quarter', 'NST Group', 'VALUE'],
                     'tem01': ['STATISTIC Label', 'Month', 'Taxation Class', 'VALUE'],
                     'naq05': ['Statistic Label', 'Quarter', 'Sector', 'VALUE']}

for table, labels in pxstat_codes_dict.items():
    data = pd.read_csv(f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{table}/CSV/1.0/en")
    try:
        data = data[labels]
        if 'Statistic Label' in data.columns:
            data.rename(columns = {'Statistic Label': 'Statistic', 'VALUE': 'value'}, inplace = True)
        elif 'STATISTIC Label' in data.columns:
            data.rename(columns = {'STATISTIC Label': 'Statistic', 'VALUE': 'value'}, inplace = True)
        data.columns = [col.replace(' ', '.') for col in data.columns]
        data.to_csv(f'/home/flanneryj/econ_dash/{table}.csv')
    except Exception as e:
        print(f"An error occured while processing table {table}: {e}")
        continue

def git_push():
    project_path = '/home/flanneryj/econ_dash'
    os.chdir(project_path)

    try:

        pat = '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'
        url = f'https://username:{pat}@github.com/flanneryjcso/econ_dash_data.git'

        subprocess.run(['git', 'remote', 'set-url', 'origin', url], check=True)
        subprocess.run(['git', 'add', '.'], check=True)
        subprocess.run(['git', 'commit', '-m', 'Automated commit of CSV files'], check=True)
        subprocess.run(['git', 'push', 'origin', 'main'], check=True)
        print("Successfully pushed to GitHub.")

    except subprocess.CalledProcessError as e:

        print("Failed to push to GitHub:", e)

git_push()
