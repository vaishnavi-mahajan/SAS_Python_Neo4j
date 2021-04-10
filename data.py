import re
import json
import pandas as pd


def convert_file(text_file, json_file):
# Opening text file
    with open(text_file, "r") as fl:
        new_file = fl.read()
# Replacing every SOH symbol     
        new_file = new_file.replace(r",", "&&")
        new_file = new_file.replace(r"", ",")
        new_file = re.sub(r",,,,", ",NaN,NaN,NaN,", new_file)
        new_file = re.sub(r",,,", ",NaN,NaN,", new_file)
        new_file = re.sub(r",,", ",NaN,", new_file)
    with open("csv_file.csv", "w") as fs:
        fs.write(new_file)
    df = pd.read_csv("csv_file.csv", header=None, error_bad_lines=False)
    print(df)
    header = read_json(json_file)
    df.fillna('NaN', inplace=True)
    try:
# Converting text to csv
        df.to_csv("csv_file.csv", header=header, index=False)
    except Exception as err:
        print(err)

# Reading json file
def read_json(json_file):
    with open(json_file, "r") as js:
        json_read = json.loads(js.read())
        header_list = []
        for j_data in json_read:
# Taking only planning_info table_name from json
            if j_data["table_name"] == "planning_info":
# Taking only column_label from planning_info table and replace space between words by underscore
                under_score = re.sub(
                    r'\s', '_', j_data['Column_label'])
                header_list.append(under_score)
        return header_list

# Giving name of text and json file in main
if __name__ == "__main__":
    convert_file("text.txt", "plan_schema.json")

# Importing py2neo to make connection with neo4j
from py2neo import Graph

#Establishing connection
graph = Graph("bolt://52.23.248.106:7687", user="neo4j", password="property-dye-labels")

#Deleting all previous states
graph.delete_all()

#Reading csv_file.csv using pandas
df = pd.read_csv("csv_file.csv")
print(df)
notna_df = df[df['Planning_Item_Parent_Name'].notna()]
notna_df

# Selecting all rows in which 'Planning_Item_Parent_Name' is null
na_df = df[df['Planning_Item_Parent_Name'].isna()]

# Importing relationships and nodes using Py2neo

from py2neo import Relationship
from py2neo import Node

# Creating set of 'Planning_Item_Parent_Name'
unique_pids = set({})

for pid in notna_df['Planning_Item_Parent_Name']:
    unique_pids.add(pid)

# Creating dict of 'Planning_Item_Parent_Name' in which key is name of 'Planning_Item_Parent_Name' and value is corresponding node.
pids = {}
for pid in unique_pids:
    pids[pid] = Node('Planning_Item_Parent_Name', name=pid)

# Adding relation between row nodes and parent node
# Iterating through each item of notna_df
for i in notna_df.index:
    pid = notna_df['Planning_Item_Parent_Name'][i]

    # We are creating a node for item_name here.
    node = Node('Item_Name', name=notna_df['Planning_Item_Name'][i])
    for col in notna_df.columns:
        # Here we are adding propertities for each item node.
        node[str(col)] = str(notna_df[col][i])
    # Creating relationship between item node and its parent name.
    graph.create(Relationship(pids[pid], "Child", node))

# Adding item nodes that do not have parent data

# Iterating through each item of na_df
for i in na_df.index:
    # We are creating a node for item_node here.
    node = Node('Item_Name_without_parent', name=na_df['Planning_Item_Name'][i])
    for col in na_df.columns:
        # Here we are adding propertities for each item node.
        node[str(col)] = str(na_df[col][i])

    # Adding item node that do not have parent.
    graph.create(node)
