# -*- coding: utf-8 -*-
from neo4j import GraphDatabase
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from io import StringIO
import requests
import os
import re
from string import Template


_ = load_dotenv(find_dotenv())
NEO_URI = os.environ['NEO_URI']
NEO_NAME = os.environ['NEO_NAME']
NEO_PASS = os.environ['NEO_PASS']


load_rdf_query = Template("""
CALL n10s.graphconfig.init();
CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE;
CALL n10s.rdf.import.fetch("file:///${rdf_file_path}", "RDF/XML");
""")

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
    Loads an RDF generated through the portal into a neo4j database. Requires the necessary
    :param rdf_file_path: The path to the rdf file.
    """
    with GraphDatabase.driver(NEO_URI, auth=(NEO_NAME, NEO_PASS)) as driver:
        driver.execute_query(
            load_rdf_query.substitute(rdf_file_path=os.path.abspath(rdf_file_path)),
            database_=NEO_NAME
        )


def minimal_data_load(dataset_uri: str):
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
    x_df.to_csv(rf'datasets/{clean_dslabel}.csv', index=False)

    # load data
    with GraphDatabase.driver(NEO_URI, auth=(NEO_NAME, NEO_PASS)) as driver:
        ds_name = 'CouncilTaxCollectionRates'
        df = pd.read_csv(f'datasets/{ds_name}.csv').iloc[:10, :]

        driver.execute_query(f'MERGE (n:dataset{{name:"{ds_name}"}})', database_=NEO_NAME)

        for i in [ii for ii in df.columns if ii not in ['value', 'measureType']]:
            if i != 'index':
                for j in df[i].unique():
                    driver.execute_query(f"MERGE (n:`{i}`{{name: '{j}'}})", database_=NEO_NAME)
            else:
                for j in df[i].unique():
                    driver.execute_query(f"MATCH (n{{name:'{ds_name}'}}) MERGE (n)<-[:member]-(m:observation{{name:'{j}'}})", database_=NEO_NAME)

        for i, j in df.iterrows():
            for val in [ii for ii in df.columns if ii not in ['measureType', 'index']]:
                if val != 'value':
                    driver.execute_query(
                        f"MATCH (a:observation{{name: '{j['index']}'}}) MATCH (b:`{val}`{{name: '{j[val]}'}}) MERGE (a)-[:`{val}`]->(b)",
                        database_=NEO_NAME)
                else:
                    driver.execute_query(
                        f"MATCH (a:observation{{name: '{j['index']}'}}) MERGE (a)-[:`{j['measureType']}`]->(b:`{val}`{{name: {j[val]}}})",
                        database_=NEO_NAME)


if __name__ == '__main__':
    minimal_data_load('http://statistics.gov.scot/data/pupil-attainment')
