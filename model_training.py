import os
import psycopg2
import json
import re
import string

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

from vacancy_analysis.utils.processing_utils import normalize_text, replace_multiword_skills


dag_path = os.path.abspath(__file__)
dag_dir_path = os.path.dirname(os.path.dirname(dag_path))
airflow_path = os.path.dirname(dag_dir_path)

DATA_PATH = os.getenv(
    "AIRFLOW_VAR_DATA_PATH",
    f"{airflow_path}/data/vacancy-analysis"
)
RAW_PATH = os.path.join(DATA_PATH, "raw"); os.makedirs(RAW_PATH, exist_ok=True)
MODEL_PATH = os.path.join(DATA_PATH, "models"); os.makedirs(MODEL_PATH, exist_ok=True)
MODEL_TRAIN_PATH = os.path.join(MODEL_PATH, "train"); os.makedirs(MODEL_TRAIN_PATH, exist_ok=True)


with DAG(
    "skills_model_training",
    default_args={
        "owner": "airflow",
    },
    schedule="@weekly",
    template_searchpath=[DATA_PATH],
    tags=["training", "tech", "skills", "model"]
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="preprocess_text")
    def preprocess_text():
        from pymorphy3 import MorphAnalyzer
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize


        morph = MorphAnalyzer()
        conn = BaseHook.get_connection("vacancy_db")
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()
        cursor.execute("SELECT skill_name FROM dim_skill")
        
        stop_words_en = set(stopwords.words('english'))
        stop_words_ru = set(stopwords.words('russian'))
        punctuation = set(string.punctuation)
        all_stop_words = stop_words_en | stop_words_ru | punctuation

        skills = [skill[0].replace(" ", "_") for skill in cursor.fetchall()]
        multiword_skills = [skill for skill in skills if "_" in skill]
        requirements = []

        for file_name in os.listdir(MODEL_TRAIN_PATH):
            file_path = os.path.join(MODEL_TRAIN_PATH, file_name)
            with open(file_path, "r") as file:
                items = json.load(file)["items"]
                for data in items:
                    replaced_skills_text = replace_multiword_skills(data["requirement"], multiword_skills)
                    tokenized_text = normalize_text(replaced_skills_text, word_tokenize, all_stop_words, morph)
                    requirements.append(tokenized_text)

        processed_data_path = os.path.join(MODEL_TRAIN_PATH, "normalized_text.json")
        with open(processed_data_path, "w") as output:
            json.dump(
                    {"data": requirements},
                    output
                )


    @task(task_id="train_model")
    def train_model():
        from gensim.models import Word2Vec

        
        processed_data_path = os.path.join(MODEL_TRAIN_PATH, "normalized_text.json")
        with open(processed_data_path, "r") as input_json:
            data = json.load(input_json)["data"]

        model_path = os.path.join(MODEL_PATH, "skills.model")
        try:
            print("Try to incrementally train model for skills")
            model = Word2Vec.load(model_path)
            model.build_vocab(data, update=True)
            model.train(data, total_examples=model.corpus_count, epochs=model.epochs)
        except Exception as e:
            print(f"There is not model for skills: {e}")
            print("Creating model")
            model = Word2Vec(
                data,
                vector_size=100,
                window=3,
                min_count=5,
            )
        model.save(model_path)


    @task(task_id="delete_train_data")
    def delete_train_data():
        for file_name in os.listdir(MODEL_TRAIN_PATH):
            file_path = os.path.join(MODEL_TRAIN_PATH, file_name)
            os.remove(file_path)


    end = EmptyOperator(task_id="end")

    preprocess_text_task = preprocess_text()
    train_model_task = train_model()
    delete_train_data_task = delete_train_data()

    start >> preprocess_text_task >> train_model_task >> delete_train_data_task >> end