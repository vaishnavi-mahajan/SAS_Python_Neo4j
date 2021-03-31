# SAS_Python_Neo4j

#import pandas to read csv file
import pandas

with open(r'C:\Users\Vaishnavi\OneDrive\Desktop\vai - Copy.txt') as f:
    my=list(f)
    
    # Replace every SOH by ,NaN,
    
    words = [w.replace('', ',NaN,') for w in my]
    
    words2= [w.replace('', ',') for w in words]
    
    #Replace every two commas(,,) by one comma(,)
    words3 = [w.replace(',,', ',') for w in words2]
    
    dff = pandas.DataFrame(words3)
    
    # Open the json file
    df = pandas.read_json(open(r'C:\Users\Vaishnavi\Downloads\plan_schema.json'))
    
    # Read data fron planning_info table name
    plan2 = df[df['table_name'] == 'planning_info']
    
    #Replace space by underscore(_)
    # rmws = plan2.replace(' ', '_', regex=True)
    
    #Take all the Column_labels from planning_info into list
    li = plan2["Column_label"].tolist()
    
    #Convert dataframe to csv and pass list as column headers
    dff.to_csv("data.csv", header=li, index=False)
    
    #After creating data.csv open the file
    file2 = pandas.read_csv(r"C:\Users\Vaishnavi\OneDrive\Desktop\data.csv")
    
    #print to see data.csv
    print(file2)
    
#import py2neo to connect python to neo4j database
from py2neo import Graph

#Establishing connection
graph = Graph("bolt://54.91.212.220:7687", user="neo4j", password="flesh-writings-assistant")

#Deleting all previous states
graph.delete_all()

#Take csv into dataframe
df = pd.read_csv(r"C:\Users\Vaishnavi\OneDrive\Desktop\data.csv")
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

# ADDING RALATIONSHIPS BETWEEN ITEM NODE AND THEIR PARENT NAME

# Iterating through each item of notna_df
for i in notna_df.index:
    pid = notna_df['Planning_Item_Parent_Name'][i]

    # We are creating a node for item_id here.
    node = Node('Item_ID', name=notna_df['Planning_Item_ID'][i])
    for col in notna_df.columns:
        # Here we are adding propertities for each item node.
        node[str(col)] = str(notna_df[col][i])
    # Creating relationship between item node and its parent name.
    graph.create(Relationship(node, "Child Of", pids[pid]))

# ADDING ITEM NODES INTO GRAPH THAT DO NOT HAVE PARENT.

# Iterating through each item of na_df
for i in na_df.index:
    # We are creating a node for item_id here.
    node = Node('Item_ID', name=na_df['Planning_Item_ID'][i])
    for col in na_df.columns:
        # Here we are adding propertities for each item node.
        node[str(col)] = str(na_df[col][i])

    # Adding item node that do not have parent.
    graph.create(node)
