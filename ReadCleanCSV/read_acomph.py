from sys import argv
import numpy as np
import pandas as pd
import glob
import os


def clear_txt(caracter_find, caracter_change, source_file, destination_file):
    csvFile = open(source_file)
    replace = ''.join([i for i in csvFile]).replace(caracter_find, caracter_change)
    openCSV=open(destination_file,"w")
    openCSV.writelines(replace)
    openCSV.close()
    

def truncate_csv(path):
    extension = 'csv'
    os.chdir(path)
    result = glob.glob('*.{}'.format(extension))
    for csv in result:
        csv=open(path+'//'+csv,"w")
        csv.truncate()
        csv.close()


def standardize_sources(df_raw):
    return df_raw.replace({"ã": "a", "Ã": "A",
                           "á": "a", "Á": "A",
                           "â": "a", "Â": "A",
                           "é": "e", "É": "E",
                           "ê": "e", "Ê": "E",
                           "í": "i", "Í": "I",
                           "ó": "o", "Ó": "O",
                           "ô": "o", "Ô": "O",
                           "ú": "u", "Ú": "U",
                           "ç": "c", "Ç": "C",
                           "²": "2", "³": "3",
                           " ": ""}, regex=True)


def get_df_from_acomph(file_directory, file_timestamp):
    df = pd.read_excel(file_directory, header=[0, 1, 2, 3, 4], sheet_name=None)
    df_acomph = pd.DataFrame()
    for sheet_name, sheet in df.items():
        date = sheet.iloc[:30, [0]]
        pos_id = 0
        for var_name, var_raw in sheet.items():
            pos_id += 1
            if type(var_name[0]) == int and "Unnamed" in var_name[1]:
                var_range = sheet.iloc[:30, (pos_id - 8):(pos_id)]
                j = 0
                for col_name, col in var_range.items():
                    col = col.to_frame()
                    col.columns = ["Value"]
                    col["Date"] = date
                    col["Basin"] = sheet_name
                    if j == 0:
                        plant_name = col_name[1]
                    j += 1
                    k = 0
                    for lvl in col_name:
                        if k == 0:
                            col[f'Lvl{k}'] = var_name[0]
                        elif k == 1:
                            col[f'Lvl{k}'] = plant_name
                        elif k == 2 and "Unnamed" in lvl:
                            col[f'Lvl{k}'] = "Vazão (m³/s)"
                        elif k == 3 and "Unnamed" in lvl:
                            col[f'Lvl{k}'] = np.NaN
                        else:
                            col[f'Lvl{k}'] = lvl
                        k += 1
                    df_acomph = pd.concat([df_acomph, col], ignore_index=True, sort=False)

    df_acomph = standardize_sources(df_acomph)
    df_acomph["TimeStamp"] = pd.Series([file_timestamp] * len(df_acomph))
    df_acomph = df_acomph.set_index(["TimeStamp", "Date", "Basin", "Lvl0", "Lvl1", "Lvl2", "Lvl3", "Lvl4"])
    df_acomph = df_acomph.unstack()
    df_acomph.columns = (["Verified", "Raw"])
    return df_acomph


if __name__ == '__main__':
    if len(argv) != 2:
        raise Exception('This script must receive a file directory as input.')
    df = get_df_from_acomph(argv[1], None)
    print(df)
