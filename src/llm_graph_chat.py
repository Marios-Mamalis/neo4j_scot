from langchain.chat_models import ChatOpenAI
from langchain.chains import GraphCypherQAChain
from langchain.graphs import Neo4jGraph
from dotenv import load_dotenv, find_dotenv
import os


_ = load_dotenv(find_dotenv())
NEO_URI = os.environ['NEO_URI']
NEO_NAME = os.environ['NEO_NAME']
NEO_PASS = os.environ['NEO_PASS']
OPENAI_API_KEY = os.environ['OPENAI_API_KEY']


def chat_over_graph_with_llm(question: str):
    graph = Neo4jGraph(url=NEO_URI, username=NEO_NAME, password=NEO_PASS)
    llm = ChatOpenAI(temperature=0)
    chain = GraphCypherQAChain.from_llm(llm, graph=graph, verbose=True)

    return chain.run(question)


if __name__ == '__main__':
    print(chat_over_graph_with_llm('Sample question'))
