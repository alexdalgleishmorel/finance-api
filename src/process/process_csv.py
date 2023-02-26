import pandas as pd

COLUMN_NAMES = ['Date', 'Description', 'Value']

def process_csv(filepath):
    debitsDataframe = pd.read_csv(filepath, names=COLUMN_NAMES)
    creditsDataframe = debitsDataframe[debitsDataframe['Value'] > 0] 

    indexesToDrop = []
    for index, row in debitsDataframe.iterrows():
        if row['Value'] > 0:
            indexesToDrop.append(index)

    for index in indexesToDrop:
        debitsDataframe.drop(index, inplace=True)

    print(debitsDataframe.to_string())
    print(creditsDataframe.to_string())

    return debitsDataframe, creditsDataframe

process_csv('../../tests/data/creditCardHistory.csv')
