#!/usr/bin/env python3.7
# --------------------------------------------------------------------------------#
# https://github.com/shinichi-takii/ddlparse/blob/master/example/example.py
# --------------------------------------------------------------------------------#
from io import open
import yaml
import pprint
# from ddlparse.ddlparse import DdlParse


# --------------------------------------------------------------------------------#
def load_ddl(file_in, file_out):

    with open(file_in, 'r') as file:
        # sample_ddl = file.read().replace('\n', '')
        sample_ddl = file.read()

    table = DdlParse().parse(sample_ddl)
    table_ddl = {"data": []}

    for col in table.columns.values():
        # print("{:26} {:26} {:26}".format(col.name, col.data_type, str(col.length)))

        user = {"name": col.name, "data_type": col.data_type, "length": col.length, "precision": col.precision,
                "not_null": col.not_null, "primary_key": col.primary_key, "unique": col.unique,
                "generator": "randomize"}

        table_ddl["data"].append(user)

    with open(file_out, 'w') as outfile:
        yaml.dump(table_ddl, outfile, default_flow_style=False)


# --------------------------------------------------------------------------------#
def load_yaml(filename):
    pp = pprint.PrettyPrinter(width=41, compact=True)

    with open(filename) as file:
        documents = yaml.full_load(file)
    pp.pprint(documents)

    v = documents.items()
    pp.pprint(v)

    # for item, doc in documents.items():
    # print(item, ":", doc)
    #    pp.pprint(doc)


# --------------------------------------------------------------------------------#
def load_slices_distribution():

    print("Slices Distribution")

    slices = []
    with open('90-Data-Definition-Language/3-slices.txt') as f:
        for line in f:
            line = line.rstrip("\n\r").split("|")
            # print(line[2])
            slices.append([ int(line[1]), int(line[2])])


    for i in slices:
        print(str(i[0]) + " | " + str(i[1]))


# --------------------------------------------------------------------------------#
if __name__ == "__main__":

    # client_in = client + ".sql"
    # client_ou = client + ".yaml"

    # print(client_in)
    # print(client_ou)
    # load_ddl(client_in, client_ou)
    # load_yaml(client_ou)

    load_slices_distribution()

# --------------------------------------------------------------------------------#
