import pandas as pd

from os import listdir
from os.path import isfile, join
# get all csv files in clean-data
onlyfiles = [f for f in listdir("clean-data") if isfile(join("clean-data", f)) and f.endswith(".csv")]

total_count = 0
total_empty_count = 0
for file in onlyfiles:
    csv_file = pd.DataFrame(pd.read_csv("clean-data/" + file, sep = ",", header = 0, index_col = False))
    if "links" in csv_file.columns:
        csv_file.rename(columns={"links": "link"}, inplace=True)

    if "Link to full text" in csv_file.columns:
        csv_file.rename(columns={"Link to full text": "link"}, inplace=True)

    if "Title" in csv_file.columns:
        csv_file.rename(columns={"Title": "name"}, inplace=True)

    # filter where act_num is null and count
    if "name" in csv_file.columns:
        empty_count = len(csv_file[csv_file['name'].isnull()])
    else:
        empty_count = len(csv_file)
    row_count = len(csv_file)
    total_count += row_count
    total_empty_count += empty_count
    print(file, row_count, empty_count, empty_count / row_count * 100)

print(f"Total rows: {total_count}")
print(f"Empty count: {total_empty_count}")
