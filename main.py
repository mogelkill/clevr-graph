from gqa.generate_graph import GraphGenerator, GraphSpec
from dataclasses import dataclass
from gqa.questions import question_forms, QuestionForm
import random 
from tqdm import tqdm
from gql.graph_builder import GraphBuilder
import yaml
import json
import ast
from typing import Dict, Any, List, Callable, Tuple,Union
from neo4j import GraphDatabase
import pandas as pd

NeoTypes = Union[int, float, str]

@dataclass
class GraphArgs:
    tiny: bool = False
    small: bool = False
    int_names:bool = False

@dataclass
class QuestionGenerationArgs:
    generate_cypher:bool = True

NODE_PROPERTIES_QUERY = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" AND elementType = "node"
WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
RETURN {labels: nodeLabels, properties: properties} AS output

"""

REL_PROPERTIES_QUERY = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" AND elementType = "relationship"
WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
RETURN {type: nodeLabels, properties: properties} AS output
"""

REL_QUERY = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE type = "RELATIONSHIP" AND elementType = "node"
UNWIND other AS other_node
RETURN "(:" + label + ")-[:" + property + "]->(:" + toString(other_node) + ")" AS output
"""

def quote(x: str):
    return f'"{x}"'

def cypherparse(x: Any):
    if isinstance(x, str):
        try:
            parsed = ast.literal_eval(x)
        except:
            return x
        x = parsed

    if isinstance(x, NeoTypes.__args__):
        return x
    else:
        print("WARNING: unsupported type", x, str(x))
        return str(x)

def cypherencode(v: NeoTypes):
    return quote(v) if isinstance(v, str) else v


def CONST_LABEL(label: str) -> Callable[[Dict[str, Any]], List[str]]:
    result = [label]

    def label_fn(entity: Dict[str, Any]):
        return result

    return label_fn

def ALL_PROPERTIES(entity: Dict[str, Any]) -> Dict[str, NeoTypes]:

    return {k: cypherparse(v) for k, v in entity.items()}

def FROM_TO(from_property: str, to_property: str) -> Callable[
    [Dict[str, Any]], Tuple[NeoTypes, NeoTypes]]:
    def route_fn(entity: Dict[str, Any]):
        return cypherparse(entity[from_property]), cypherparse(entity[to_property])

    return route_fn

def generate_node_inserts(graphSpec:GraphSpec):
    node_label_fn = CONST_LABEL("STATION")
    node_prop_fn = ALL_PROPERTIES
    for key,node in graphSpec.nodes.items():
        labels = node_label_fn(node)
        props = node_prop_fn(node.state)

        props = ', '.join(
            f'{k}: {quote(v) if isinstance(v, str) else v}' for k, v in props.items())
        template = f"CREATE (n:{':'.join(labels)} {{ {props} }})"
        yield template

    for key,line in graphSpec.lines.items():
        props = node_prop_fn(line.state)
        props = ', '.join(
            f'{k}: {quote(v) if isinstance(v, str) else v}' for k, v in props.items())
        template = f"CREATE (n:LINE {{ {props} }})"
        yield template

def generate_edge_inserts(graphSpec:GraphSpec):
    edge_label_fn = CONST_LABEL("EDGE")
    edge_prop_fn = ALL_PROPERTIES
    edge_route_fn = FROM_TO("station1", "station2")
    for edge in graphSpec.edges:
        labels = edge_label_fn(edge)
        assert len(labels) > 0, "edges must have at least one label"
        props = edge_prop_fn(edge.state)
        props = ', '.join(
            f'{k}: {cypherencode(v)}' for k, v in props.items()
        )
        from_id, to_id = edge_route_fn(edge)

        template = f"MATCH (from),(to) " \
                    f"WHERE from.id = {cypherencode(from_id)} " \
                    f"and to.id = {cypherencode(to_id)} " \
                    f"CREATE (from)-[l:{':'.join(labels)} {{ {props} }}]->(to)"

        yield template

def get_schema(self) -> str:
    if self.schema is None:
        node_properties = self.execute(NODE_PROPERTIES_QUERY)
        relationships_properties = self.execute(REL_PROPERTIES_QUERY)
        relationships = self.execute(REL_QUERY)

        self.schema = f"""
        Node properties are the following:
        {[el['output'] for el in node_properties]}
        Relationship properties are the following:
        {[el['output'] for el in relationships_properties]}
        The relationships are the following:
        {[el['output'] for el in relationships]}
        """

    return self.schema

def nuke_neo(session):
    session.write_transaction(lambda tx: tx.run("MATCH ()-[r]-() delete r"))
    session.write_transaction(lambda tx: tx.run("MATCH (n) delete n"))

if __name__=="__main__":
    generator = GraphGenerator(GraphArgs())
    generator = generator.generate()

    driver=GraphDatabase.driver("bolt://localhost:7687", auth=("", ""))
    node_inserts = list(generate_node_inserts(generator.graph_spec))
    edge_inserts = list(generate_edge_inserts(generator.graph_spec))

    total_inserts = node_inserts + edge_inserts

    nuke_neo(driver.session())
    for insert_statement in tqdm(total_inserts,desc="Inserting nodes and edges"):
        driver.session().execute_write(lambda tx: tx.run(insert_statement))

    def get_random_question_template()->QuestionForm:
        return random.choice(question_forms)

    g = generator.graph_spec
    questions = []
    for i in (bar := tqdm(range(10000), desc="Generating questions")):
        try:
            question_template = get_random_question_template()
            question, answer = question_template.generate(g,QuestionGenerationArgs())
            if question.cypher is None:
                continue
            payload = {
                "question":question.english,
                "cypher":question.cypher,
                "group":question.group,
                "answer":answer,
            }
            questions.append(json.dumps(payload))
            #bar.set_description(f"Generated question: {question}, Answer: {answer}")
        except Exception as e:
            pass    

    with open("questions.jsonl","w") as file:
        file.write("\n".join(questions))
    
    questions_df=pd.read_json("questions.jsonl",lines=True)
    print(questions_df.head())
