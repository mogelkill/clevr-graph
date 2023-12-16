from gqa.generate_graph import GraphGenerator
from dataclasses import dataclass
from gqa.questions import question_forms, QuestionForm
import random 
from tqdm import tqdm

@dataclass
class GraphArgs:
    tiny: bool = False
    small: bool = False
    int_names:bool = False

@dataclass
class QuestionGenerationArgs:
    generate_cypher:bool = True

generator = GraphGenerator(GraphArgs())
generator = generator.generate()


def get_random_question_template()->QuestionForm:
    return random.choice(question_forms)

g = generator.graph_spec

for i in (bar := tqdm(range(100000), desc="Generating questions")):
    try:
        question_template = get_random_question_template()
        print(question_template.english)
        question, answer = question_template.generate(g,QuestionGenerationArgs())
        bar.set_description(f"Generated question: {question}, Answer: {answer}")
    except Exception as e:
        pass
