# -*- coding: utf-8 -*-
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from io import StringIO
import requests
import os
import re
from string import Template


_ = load_dotenv(find_dotenv())
NEO_URI = os.environ['NEO_URI']
NEO_USERNAME = os.environ['NEO_USERNAME']
NEO_PASS = os.environ['NEO_PASS']
NEO_DATABASE = os.environ['NEO_DATABASE']


prefixes = """
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""

get_dataset_label = Template(f"""
{prefixes}
SELECT distinct ?ds ?dsl
WHERE {{
    <${{dataset_uri}}> rdfs:label ?dsl.
    ?ds rdfs:label ?dsl
    }}
""")

get_dims = Template(f"""
{prefixes}

SELECT distinct *
WHERE {{
<${{dataset_uri}}> qb:structure/qb:component/qb:dimension ?dim.
  optional{{?dim rdfs:label ?label.}}
}}
""")


def import_from_rdf(rdf_file_path: str) -> None:
    """
    Loads an RDF generated through the portal into a Neo4j database.
    :param rdf_file_path: The path to the rdf file.
    """
    with GraphDatabase.driver(NEO_URI, auth=(NEO_USERNAME, NEO_PASS)) as driver:
        driver.execute_query('CALL n10s.graphconfig.init();', database_=NEO_DATABASE)
        try:
            driver.execute_query('CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE;', database_=NEO_DATABASE)
        except ClientError:
            pass
        driver.execute_query(f'CALL n10s.rdf.import.fetch("file:///{os.path.abspath(rdf_file_path)}", "RDF/XML");', database_=NEO_DATABASE)


def load_dataset(dataset_uri: str) -> None:
    """
    Loads a dataset from the portal into a Neo4j database.
    :param dataset_uri: The uri of the dataset, as seen on the portal.
    """

    if dataset_uri.startswith('https'):
        dataset_uri = 'http' + dataset_uri[5:]

    # get data
    d = requests.get(f'http://statistics.gov.scot/sparql.csv', params={'query': get_dataset_label.substitute(dataset_uri=dataset_uri)})
    dataset_label = d.text.splitlines()[1].split(',')[1]
    clean_dslabel = re.sub('[^A-Za-z0-9]', '', dataset_label)

    x = requests.get(f'http://statistics.gov.scot/sparql.csv', params={'query': get_dims.substitute(dataset_uri=dataset_uri)})
    x_df = pd.read_csv(StringIO(x.text), sep=",", dtype=str)
    x_df = x_df[~x_df['dim'].str.contains('measureType')]
    dims = x_df['dim'].tolist()
    dim_labels = x_df['label'].tolist()

    data_query = f"{prefixes}\nSELECT distinct ?obs {' '.join(['?x' + str(j) for j in range(len(dims))])} ?measureType ?value\nWHERE {{\n?obs qb:dataSet <{dataset_uri}>;"
    for i, it in enumerate(dims):
        data_query += f"<{it}>/rdfs:label ?x{i};\n"
    data_query += "qb:measureType/rdfs:label ?measureType;\nqb:measureType ?measureTypev;\n?measureTypev ?value\n}"

    x = requests.get(f'http://statistics.gov.scot/sparql.csv', params={'query': data_query})
    x_df = pd.read_csv(StringIO(x.text), sep=",", dtype=str)
    x_df = x_df.rename(columns=dict(
        zip(['x' + str(j) for j in range(len(dims))], [re.sub('[^A-Za-z0-9]', '', ii) for ii in dim_labels])))
    x_df.reset_index(inplace=True)
    x_df.drop(['obs'], axis=1, inplace=True)
    x_df['index'] = x_df['index'].apply(lambda g: clean_dslabel + str(g))

    # load data
    with GraphDatabase.driver(NEO_URI, auth=(NEO_USERNAME, NEO_PASS)) as driver:
        driver.execute_query(f'MERGE (n:dataset{{name:"{clean_dslabel}"}})', database_=NEO_DATABASE)

        for i in [ii for ii in x_df.columns if ii not in ['value', 'measureType']]:
            if i != 'index':
                for j in x_df[i].unique():
                    driver.execute_query(f"MERGE (n:`{i}`{{name: '{j}'}})", database_=NEO_DATABASE)
            else:
                for j in x_df[i].unique():
                    driver.execute_query(f"MATCH (n{{name:'{clean_dslabel}'}}) MERGE (n)<-[:member]-(m:observation{{name:'{j}'}})", database_=NEO_DATABASE)

        for i, j in x_df.iterrows():
            for val in [ii for ii in x_df.columns if ii not in ['measureType', 'index']]:
                if val != 'value':
                    driver.execute_query(
                        f"MATCH (a:observation{{name: '{j['index']}'}}) MATCH (b:`{val}`{{name: '{j[val]}'}}) MERGE (a)-[:`{val}`]->(b)",
                        database_=NEO_DATABASE)
                else:
                    driver.execute_query(
                        f"MATCH (a:observation{{name: '{j['index']}'}}) MERGE (a)-[:`{j['measureType']}`]->(b:`{val}`{{name: {j[val]}}})",
                        database_=NEO_DATABASE)
