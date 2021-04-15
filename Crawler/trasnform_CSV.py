from sys import argv
import numpy as np
import pandas as pd

def standardizeSources(df_raw):
    """
    function used to standardize source text.
    usarg:
        --df_raw(str): Storage (Bucket) of data source and destination
    """
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


def getDfFromProcess(fileDirectory, fileTimestamp):
    """
    function used to standardize source text.
    usarg:
        --df_raw(str): Storage (Bucket) of data source and destination
    """
    df = pd.read_excel(fileDirectory, header=[0, 1, 2, 3, 4], sheetName=None)
    dfProcess = pd.DataFrame()
    for sheetName, sheet in df.items():
        date = sheet.iloc[:30, [0]]
        posID = 0
        for varName, var_raw in sheet.items():
            posID += 1
            if type(varName[0]) == int and "Unnamed" in varName[1]:
                var_range = sheet.iloc[:30, (posID - 8):(posID)]
                j = 0
                for colName, col in var_range.items():
                    col = col.to_frame()
                    col.columns = ["Value"]
                    col["Date"] = date
                    col["Basin"] = sheetName
                    if j == 0:
                        plantName = colName[1]
                    j += 1
                    k = 0
                    for lvl in colName:
                        if k == 0:
                            col[f'Lvl{k}'] = varName[0]
                        elif k == 1:
                            col[f'Lvl{k}'] = plantName
                        elif k == 2 and "Unnamed" in lvl:
                            col[f'Lvl{k}'] = "Vazão (m³/s)"
                        elif k == 3 and "Unnamed" in lvl:
                            col[f'Lvl{k}'] = np.NaN
                        else:
                            col[f'Lvl{k}'] = lvl
                        k += 1
                    dfProcess = pd.concat([dfProcess, col], ignore_index=True, sort=False)

    dfProcess = standardizeSources(dfProcess)
    dfProcess["TimeStamp"] = pd.Series([fileTimestamp] * len(dfProcess))
    dfProcess = dfProcess.set_index(["TimeStamp", "Date", "Basin", "Lvl0", "Lvl1", "Lvl2", "Lvl3", "Lvl4"])
    dfProcess = dfProcess.unstack()
    dfProcess.columns = (["Verified", "Raw"])
    return dfProcess


"""
Execute function
"""
if __name__ == '__main__':
    if len(argv) != 2:
        raise Exception('This script must receive a file directory as input.')
    df = getDfFromProcess(argv[1], None)
    print(df)
