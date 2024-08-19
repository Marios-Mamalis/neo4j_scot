# Neo4j Scot
A set of utilities for loading data from statistics.gov.scot to Neo4j.

## Installation
Requires Python 3.9
```
pip install -r requirements.txt
```

The Neo4j database must also have the `neosemantics`(n10s) plugin
[installed](https://neo4j.com/labs/neosemantics/installation/) in case raw RDF 
data are to be stored with `main.import_from_rdf`.

The example of LLM usage on the created graph (`llm_inference_on_graph.py`) also requires
the APOC Neo4j library.