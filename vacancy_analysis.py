import requests
import psycopg2
import json
import csv
import re
import pandas as pd
import html
import time
import numpy as np
import gensim.downloader as api
import nltk
import os
import string

from geopy.geocoders import Nominatim
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Tuple, Dict
from datetime import datetime
from bs4 import BeautifulSoup

from airflow.sdk import DAG, task, task_group

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.hooks.base import BaseHook
from airflow.models import Variable

DATA_PATH = os.getenv(
    "AIRFLOW_VAR_DATA_PATH",
    "/home/ruzal/Desktop/utils/airflow/data/vacancy-analysis"
)
CURRENCY_TOKEN = Variable.get("currency_token")


def convert_with_checking(func: object, source: dict, target: str) -> None | object:
    '''
        Функция конертации с проверкой на наличие поля в source.
        
        Аргументы:
        - func: функция приведения к типу (`int`, `str`, `float` и т.д.).
        - source: словарь, в котором нужно проверить на наличие поля.
        - target: поле в словаре.

        Вывод:  
        Сконвертированное значение ключа поля или `None`.
    '''

    if source:
        value = source[target]
        if value:
            return func(value)
        return None
    return None


def convert_to_RUB(value: int, from_currency: str, currency_values: dict) -> int:
    """
        Функция конвертации денег в российские рубли.

        Аргументы:
        - value: значение валютю.
        - from_currency: из какой валюты переводится.
        - currency_values: словарь с конвертацией 1 доллара в разные валюты

        Выход:  
        1 валюта `from_currency` в рублях.
    """

    RUB_value = float(currency_values["RUB"])
    from_currency_value = float(currency_values[from_currency])
    to_USD = value / from_currency_value
    return int(to_USD * RUB_value)


def get_proper_skill_getmatch(skill_tag: str) -> list[str]:
    """
        Функция получения навыка из тега навыков getmatch.  
        Разделяет тег по символу `/`, убирает слова слова,  
        длины которых больше 45. Если будут перечислены версии  
        технологии, например, PHP 7/8/9, то оставляет только PHP.

        Аргументы:
        - skill_tag: Навык, который написан в теге навыков вакансии  
            в getmatch.
        
        Выход:  
        Список с навыками, выделенные с тега.
    """
    
    result = []
    splitted = [word.strip() for word in skill_tag.split("/")]
    i = 0
    changed = False
    while i != len(splitted):
        if splitted[i].isdigit():
            if not(changed) and result:
                result[-1] = re.sub(r'\d+$', '', result[-1]).strip()
                changed = True
        else:
            if len(splitted[i]) <= 45:
                result.append(splitted[i])
                changed = False
        i += 1
    return result


def extract_requirements_segment(html: str) -> str:
    """
        Функция выделения из описания вакансии сегмента с  
        требованиями к позиции.

        Аргументы:
        - html: html-документ с описанием вакансии.

        Выход:  
        Сегмент текста с требованиями.
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Удалим скрипты, стили и ненужное
    for tag in soup(['script', 'style']):
        tag.decompose()

    text = soup.get_text(separator='\n')
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    # Ключевые маркеры начала требований
    requirement_headers = [
        r'Требования',
        r'Что мы ожидаем',
        r'Что ты умеешь',
        r'Нам нужен',
        r'Подойдет кандидат',
        r'Ожидания',
        r'Что нам важно',
        r'Технические навыки',
        r'Ваш опыт'
    ]
    pattern = re.compile('|'.join(requirement_headers))

    start_idx = None
    end_idx = None

    # Найдём начало сегмента
    for i, line in enumerate(lines):
        if pattern.search(line):
            start_idx = i
            break

    if start_idx is not None:
        # Ограничим до следующего заголовка (условия, задачи, мы предлагаем и т.п.)
        end_headers = [
            r'Обязанности',
            r'Что делать',
            r'Условия',
            r'Предлагаем',
            r'Компания',
            r'О нас',
            r'Контакты',
            r'Офис',
            r'Зарплата',
            r'Преимущества'
        ]
        end_pattern = re.compile('|'.join(end_headers))

        for j in range(start_idx + 1, len(lines)):
            if end_pattern.search(lines[j]):
                end_idx = j
                break

        selected = lines[start_idx:end_idx] if end_idx else lines[start_idx:]
        return ' '.join(selected)
    
    return ' '.join(lines[5:30])


def get_similar_skills(vacancies_id, text, model, skills_in_model, tech_skills, all_stop_words):
    lower_text = text.lower()
    
    tokens = [t for t in word_tokenize(lower_text) if t in model]
    
    tokens = [t for t in tokens if t.lower() not in all_stop_words and t.strip() != '']

    token_matrix = np.array([model[t] for t in tokens])
    skill_matrix = np.array([model[s.lower()] for s in skills_in_model])

    similarities = cosine_similarity(token_matrix, skill_matrix)

    found_skills = {skills_in_model[j]
                    for i in range(len(tokens))
                    for j in range(len(skills_in_model))
                    if similarities[i, j] > 0.85}
    
    return [(vacancies_id, tech_skills[skill]) for skill in found_skills]


def makefile(directory: str, filename: str) -> str:
    '''
        Функция для создания недостающих катологов для файла.  

        Аргументы:  
        - directory: название папки, в которой будет находиться filename
        - filename: название файла

        Выход:  
        Путь вида '{DATA_PATH}/directory/filename'
    '''
    output_dir = os.path.join(DATA_PATH, directory)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, filename)
    return output_file


with DAG(
    "vacancy_analysis",
    default_args={
        "owner": "airflow",
    },
    schedule=None
) as dag:
    vacancies_column_names = [
        'id', 'area', 'latitude', 'longitude', 'archived',
        'created_at', 'published_at', 'has_test', 'internship',
        'salary_from', 'salary_to', 'salary_frequency', 'company_id',
        'experience', 'employment'
    ]
    employers_column_names = [
        'id', 'name', 'total_rating', 'reviews_count',
        'accredited_it_employer', 'trusted', 'logo_url'
    ]
    vacancy_roles_column_names = [
        'vacancy_id', 'role_id'
    ]
    vacancy_work_formats_column_names = [
        'vacancy_id', 'work_format'
    ]
    vacancy_skills_column_names = [
        "vacancy_id", "skill_id"
    ]

    @task(task_id="extract_vacancies_from_api")
    def extract_vacancies_from_api():
        conn = BaseHook.get_connection("vacancy_db")
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM roles")
        roles = dict([(row[0], row[1]) for row in cursor.fetchall()])
        all_roles_id = roles.keys()

        all_vacancies = []
        for id in all_roles_id:
            vacancies = []
            i = 0
            response = requests.get(f"https://api.hh.ru/vacancies?area=113&per_page=100&page={i}&order_by=publication_time&ored_clusters=true&professional_role={id}&period=1")
            response_data = response.json()
            print(f"{roles[id]}: {response.status_code}, {response.reason}, {response_data['found']}")
            vacancies += response_data["items"]
            if to := response_data["found"] // 100:
                for i in range(1, to + 1):
                    response = requests.get(f"https://api.hh.ru/vacancies?area=113&per_page=100&page={i}&order_by=publication_time&ored_clusters=true&professional_role={id}&period=1").json()
                    vacancies += response["items"]
            all_vacancies.append({"role_id": id, "vacancies": vacancies})
        
        to_dump = {"items": all_vacancies}

        output_file = os.path.join(DATA_PATH, "extracted_vacancies.json")
        with open(output_file, 'w', encoding='utf-16') as file:
            json.dump(to_dump, file)
        
        return output_file

        
    @task(task_id="fetch_currency_rates")
    def fetch_currency_rates():
        response = requests.get(f"https://api.currencyfreaks.com/v2.0/rates/latest?apikey={CURRENCY_TOKEN}")
        currency_values = response.json()["rates"]

        output_file = makefile("raw", "currency_rates.json")

        with open(output_file, "w") as f:
            json.dump(currency_values, f)

        return output_file

    
    @task(task_id="normalize_vacancies")
    def normalize_vacancies(**context):
        vacancies_json_path = context["ti"].xcom_pull(task_ids="extract_vacancies_from_api")
        currency_file = context["ti"].xcom_pull(task_ids="fetch_currency_rates")
        
        with open(currency_file, "r") as f:
            currency_values = json.load(f)
        with open(vacancies_json_path, "r", encoding="utf-16") as f:
            data = json.load(f)
        normalized = []

        for role_data in data["items"]:
            for vacancy in role_data["vacancies"]:
                if not("id" in vacancy["employer"]): continue
                
                salary_from = convert_with_checking(int, vacancy["salary_range"], "from")
                salary_to = convert_with_checking(int, vacancy["salary_range"], "to")

                if vacancy["salary_range"] and (currency := vacancy["salary_range"]["currency"]) != "RUR":
                    if salary_from:
                        salary_from = convert_to_RUB(salary_from, currency, currency_values)
                    if salary_to:
                        salary_to = convert_to_RUB(salary_to, currency, currency_values)
                
                normalized.append([
                    int(vacancy["id"]),
                    vacancy["area"]["name"],
                    convert_with_checking(float, vacancy["address"], "lat"),
                    convert_with_checking(float, vacancy["address"], "lng"),
                    vacancy["archived"],
                    datetime.fromisoformat(vacancy["created_at"]),
                    datetime.fromisoformat(vacancy["published_at"]),
                    vacancy["has_test"],
                    vacancy["internship"],
                    salary_from,
                    salary_to,
                    vacancy["salary_range"]["frequency"]["name"].replace('\xa0', ' ') if vacancy["salary_range"] and vacancy["salary_range"]["frequency"] else "Неизвестно",
                    int(vacancy["employer"]["id"]),
                    vacancy["experience"]["name"],
                    vacancy["employment"]["name"]
                ])
        
        output_file = makefile("raw", "vacancies.csv")

        with open(output_file, "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows([vacancies_column_names] + normalized)

        return output_file


    @task(task_id="normalize_employers")
    def normalize_employers(**context):
        vacancies_json_path = context["ti"].xcom_pull(task_ids="extract_vacancies_from_api")
        with open(vacancies_json_path, "r", encoding="utf-16") as f:
            data = json.load(f)
        normalized = {}

        for role_data in data["items"]:
            for vacancy in role_data["vacancies"]:
                if not("id" in vacancy["employer"]): continue

                employer = normalized.get(vacancy["employer"]["id"], [])
                if not employer:
                    normalized[int(vacancy["employer"]["id"])] = [
                        vacancy["employer"]["name"],
                        None if not("employer_rating" in vacancy["employer"]) else convert_with_checking(float, vacancy["employer"]["employer_rating"], "total_rating"),
                        None if not("employer_rating" in vacancy["employer"]) else convert_with_checking(int, vacancy["employer"]["employer_rating"], "reviews_count"),
                        vacancy["employer"]["accredited_it_employer"],
                        vacancy["employer"]["trusted"],
                        vacancy["employer"]["logo_urls"]["original"] if vacancy["employer"]["logo_urls"] else None
                    ]
                else:
                    rating = None if not("employer_rating" in vacancy["employer"]) else convert_with_checking(float, vacancy["employer"]["employer_rating"], "total_rating")
                    reviews = None if not("employer_rating" in vacancy["employer"]) else convert_with_checking(int, vacancy["employer"]["employer_rating"], "reviews_count")
                    log_url = vacancy["employer"]["logo_urls"]["original"] if vacancy["employer"]["logo_urls"] else None
                    if not employer[1] and rating:
                        employer[1] = rating
                    if not employer[2] and reviews:
                        employer[2] = reviews
                    if not employer[5] and log_url:
                        employer[5] = log_url

        employers = [[key] + value for key, value in normalized.items()]

        output_file = makefile("raw", "employers.csv")

        with open(output_file, "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows([employers_column_names] + employers)

        return output_file


    @task(task_id="normalize_vacancy_role")
    def normalize_vacancy_role(**context):
        vacancies_json_path = context["ti"].xcom_pull(task_ids="extract_vacancies_from_api")
        with open(vacancies_json_path, "r", encoding="utf-16") as f:
            data = json.load(f)
        normalized = []

        for role_data in data["items"]:
            for vacancy in role_data["vacancies"]:
                if not("id" in vacancy["employer"]): continue

                normalized.append([
                    int(vacancy["id"]),
                    role_data["role_id"]
                ])
        
        output_file = makefile("raw", "vacancy_roles.csv")
        
        with open(output_file, "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows([vacancy_roles_column_names] + normalized)

        return output_file
        

    @task(task_id="normalize_vacancy_work_formats")
    def normalize_vacancy_work_formats(**context):
        vacancies_json_path = context["ti"].xcom_pull(task_ids="extract_vacancies_from_api")
        with open(vacancies_json_path, "r", encoding="utf-16") as f:
            data = json.load(f)
        normalized = []

        for role_data in data["items"]:
            for vacancy in role_data["vacancies"]:
                if not("id" in vacancy["employer"]): continue

                normalized += [
                    [int(vacancy["id"]), work_format["name"].replace("\xa0", " ")]
                    for work_format in vacancy["work_format"]
                ]

        output_file = makefile("raw", "vacancy_work_formats.csv")
        
        with open(output_file, "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows([vacancy_work_formats_column_names] + normalized)

        return output_file


    @task(task_id="drop_invalid_salaries")
    def drop_invalid_salaries(**context):
        normalized_vacancies_csv = context["ti"].xcom_pull(task_ids="normalize_vacancies")
        vacancies_df = pd.read_csv(normalized_vacancies_csv)
        
        print(vacancies_df.isnull().sum())
        nulls_id = vacancies_df[
            (vacancies_df['salary_from'].isnull() | vacancies_df['salary_to'].isnull())
        ]["id"]
        
        vacancies_df.drop(nulls_id.index, inplace=True)
        print(vacancies_df.isnull().sum())

        output_file = makefile("processed", "vacancies.csv")
        vacancies_df.to_csv(output_file, index=False)
        
        return output_file


    @task(task_id="fill_null_areas_and_generate_sql")
    def fill_null_areas(**context):
        conn = BaseHook.get_connection("vacancy_db")
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()

        vacancies_without_invalid_salaries = context["ti"].xcom_pull(task_ids="drop_invalid_salaries")
        vacancies_df = pd.read_csv(vacancies_without_invalid_salaries)
        loc = Nominatim(user_agent="GetLoc")
        areas_to_fix = vacancies_df[(vacancies_df['latitude'].isnull()) | (vacancies_df['longitude'].isnull())]["area"].unique()

        cursor.execute("SELECT name, latitude, longitude FROM area_coordinates")

        coordinates = {area[0]: [float(area[1]), float(area[2])] for area in cursor.fetchall()}
        new_coordinates = {}
        for area in areas_to_fix:
            if area not in coordinates:
                getLoc = loc.geocode(area)
                if getLoc:
                    latitude = getLoc.latitude
                    longitude = getLoc.longitude
                    coordinates[area] = [latitude, longitude]
                    new_coordinates[area] = [latitude, longitude]

        output_sql = None
        if new_coordinates:
            output_sql = os.path.join(DATA_PATH, "processed", "add_new_coordinates.sql")
            os.makedirs(os.path.dirname(output_sql), exist_ok=True)
            with open(output_sql, "w") as file:
                file.write("INSERT INTO area_coordinates (name, latitude, longitude) VALUES\n")
                file.write(
                    ",\n".join(
                        [
                            f"('{name}', {latitude}, {longtitude})"
                            for (name, (latitude, longtitude)) in new_coordinates.items()
                        ]
                    ) + ";"
                )

        if len(coordinates.keys()) == len(areas_to_fix):
            print("[&] Есть все координаты, меняем null...")
        else:
            print(f"[-] Количество ненайденных местностей: {len(areas_to_fix) - len(coordinates)}")
            print("[-] Сбрасываем их.", end=" ")
            null_areas = vacancies_df[
                ~vacancies_df['area'].isin(coordinates.keys())
            ]["id"].copy()
            vacancies_df.drop(null_areas.index, inplace=True)
            print(f"Количество сброшенных строк: {len(null_areas.index)}")
            print("[&] Меняем null...")

        vacancies_df.loc[:, ['latitude', 'longitude']] = vacancies_df["area"].apply(lambda x: coordinates[x]).values.tolist()

        output_vacancies = makefile("processed", "vacancies_with_fixed_areas.csv")
        
        vacancies_df.to_csv(output_vacancies, index=False)
        return {"vacancies_path": output_vacancies, "sql_path": output_sql}


    @task.branch(task_id="check_new_coordinates")
    def check_new_coordinates(**context):
        result = context["ti"].xcom_pull(task_ids="fill_null_areas_and_generate_sql", key="sql_path")
        if result:
            return "add_new_coordinates_to_db"
        return "skip_add_coordinates"


    @task(task_id="parsing_skills_from_habr")
    def parsing_skills_from_habr():
        i = 1
        parsed_skills = set()
        while True:
            habr_response = requests.get(f"https://career.habr.com/vacancies?page={i}&s[]=22&s[]=17&s[]=18&s[]=183&s[]=19&s[]=187&s[]=20&s[]=89&s[]=108&s[]=129&s[]=130&s[]=51&s[]=52&s[]=53&s[]=102&s[]=103&s[]=104&s[]=120&s[]=121&s[]=113&s[]=132&s[]=131&s[]=179&s[]=49&s[]=45&s[]=46&s[]=50&s[]=47&s[]=48&s[]=101&s[]=112&s[]=44&s[]=125&s[]=177&s[]=175&s[]=126&s[]=78&s[]=21&s[]=172&s[]=174&s[]=79&s[]=173&s[]=80&s[]=176&s[]=81&s[]=118&s[]=182&s[]=32&s[]=33&s[]=34&s[]=119&s[]=185&s[]=36&s[]=186&s[]=37&s[]=110&s[]=94&s[]=23&s[]=24&s[]=30&s[]=25&s[]=27&s[]=26&s[]=90&s[]=28&s[]=91&s[]=92&s[]=29&s[]=93&s[]=122&s[]=31&s[]=109&s[]=98&s[]=41&s[]=42&s[]=43&s[]=168&s[]=99&s[]=76&s[]=96&s[]=97&s[]=95&s[]=100&s[]=133&s[]=111&s[]=12&s[]=10&s[]=13&s[]=87&s[]=11&s[]=14&s[]=15&s[]=16&s[]=107&s[]=2&s[]=3&s[]=4&s[]=82&s[]=72&s[]=5&s[]=75&s[]=6&s[]=1&s[]=77&s[]=7&s[]=83&s[]=84&s[]=73&s[]=8&s[]=85&s[]=86&s[]=188&s[]=178&s[]=106&type=all")
            soup = BeautifulSoup(habr_response.text, 'html.parser')
            tags = soup.select('.vacancy-card__skills a')
            if not tags:
                break
            for tag in tags:
                parsed_skills.add(tag.text.strip().lower())
            i += 1
            if i % 10 == 0:
                print(i)

        habr_skills_file = makefile("raw", "habr_skills.txt")
        
        with open(habr_skills_file, "w") as file:
            file.write("\n".join(parsed_skills))
        
        return habr_skills_file


    @task(task_id="parsing_skills_from_getmatch")
    def parsing_skills_from_getmatch():
        response = requests.get("https://getmatch.ru/vacancies?p=1&sa=150000&pa=all&s=landing_ca_vacancies")
        soup = BeautifulSoup(response.content, 'html.parser')

        parsed_skills = set()

        pages = []
        for page_num in soup.find_all(class_='b-pagination-page ng-star-inserted'):
            if (num := page_num.text.strip()).isdigit(): pages.append(int(num))
        max_page = max(pages)

        for page in range(1, max_page+1):
            response = requests.get(f"https://getmatch.ru/vacancies?p={page}&sa=150000&pa=all&s=landing_ca_vacancies")
            soup = BeautifulSoup(response.content, 'html.parser')
            for skill_tag in soup.select('div.b-vacancy-card-subtitle__stack span'):
                lowered_tag = skill_tag.text.strip().lower()
                for skill in get_proper_skill_getmatch(re.sub(r"\([\w\W]+\)", '', lowered_tag)):
                    parsed_skills.add(skill.strip())
            if page % 10 == 0:
                print(f"{page} done")

        getmatch_skills_file = makefile("raw", "getmatch_skills.txt")
        
        with open(getmatch_skills_file, "w") as file:
            file.write("\n".join(parsed_skills))
        
        return getmatch_skills_file


    @task(task_id="extract_description_and_skills_from_hhru")
    def extract_description_and_skills_from_hhru(**context):
        vacancies_path = context["ti"].xcom_pull(task_ids="fill_null_areas_and_generate_sql", key="vacancies_path")
        vacancies_df = pd.read_csv(vacancies_path)
        vacancy_ids = vacancies_df["id"].values
        requirements = []
        skills = set()
        i = 0
        while i != len(vacancy_ids):
            response = requests.get(f"https://api.hh.ru/vacancies/{vacancy_ids[i]}")
            if response.status_code == 200:
                data = response.json()
                segmented_description = extract_requirements_segment(data["description"]).lower()
                for skill in data["key_skills"]:
                    if len(skill) <= 45 and len(skill.split()) <= 3:
                        skills.add(skill.lower())
                requirements.append((vacancy_ids[i], segmented_description))
            elif response.status_code == 403:
                print(f"Captcha at {i}'s request. Sleeping 10 seconds")
                time.sleep(10)
                continue
            if i % 100 == 0:
                print(i, "Done")
            i += 1

        requirements_json = makefile("raw", "vacancies_requirements.json")

        requirements_to_save = []
        for vacancy_id, requirement in requirements:
            requirements_to_save.append({"vacancy_id": vacancy_id, "requirement": requirement})
        to_save = {"items": requirements_to_save}

        with open(requirements_json, 'w') as file:
            json.dump(to_save, file)

        hhru_skills_file = makefile("raw", "hhru_skills.txt")
        
        with open(hhru_skills_file) as file:
            file.write("\n".join(skills))
        
        return {
            "requirements": requirements_json,
            "skills": hhru_skills_file
        }


    @task(task_id="union_parsed_skills_and_generate_sql")
    def union_parsed_skills_and_generate_sql(**context):
        habr_file = context["ti"].xcom_pull(task_ids="parsing_skills_from_habr")
        getmatch_file = context["ti"].xcom_pull(task_ids="parsing_skills_from_getmatch")
        hhru_file = context["ti"].xcom_pull(task_ids="extract_description_and_skills_from_hhru", key="skills")

        conn = BaseHook.get_connection("vacancy_db")
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()
        cursor.execute("select id, name from skills")
        
        tech_skills = dict([(name, id) for id, name in cursor.fetchall()])
        skills = set(tech_skills.keys())
        parsed_skills = set([])
        
        habr_file = open(habr_file)
        getmatch_file = open(getmatch_file)

        parsed_skills = parsed_skills | set([skill for skill in habr_file])
        parsed_skills = parsed_skills | set([skill for skill in getmatch_file])

        habr_file.close()
        getmatch_file.close()

        new_skills = parsed_skills.difference(skills)
        insert_skills_sql = None

        if len(new_skills) != 0:
            insert_skills_sql = makefile("processed", "insert_skills.sql")

            with open(insert_skills_sql, "w") as file:
                file.write("INSERT INTO skills (name) VALUES\n")
                file.write(",\n".join([f"('{skill}')" for skill in parsed_skills.difference(skills)]) + ";")
        
        return insert_skills_sql


    @task.branch(task_id="check_new_skills")
    def check_new_skills(**context):
        insert_skills_sql = context["ti"].xcom_pull(task_ids="union_parsed_skills_and_generate_sql")
        if insert_skills_sql:
            return "add_new_skills_to_db"
        return "skip_add_skills"


    @task(task_id="get_skills_from_requirements")
    def get_skills_from_requirements():
        requirements_json = context["ti"].xcom_pull(task_ids="extract_description_and_skills_from_hhru", key="requirements")

        conn = BaseHook.get_connection("vacancy_db")

        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
        cursor = connection.cursor()
        cursor.execute("select id, name from skills")
        
        tech_skills = dict([(name, id) for id, name in cursor.fetchall()])

        with open(requirements_json, 'r') as file:
            vacancy_requirements = json.loads(file.read())["items"]

        print("Model initialization")
        model = api.load("fasttext-wiki-news-subwords-300")
        print("Model is ready")

        stop_words_en = set(stopwords.words('english'))
        stop_words_ru = set(stopwords.words('russian'))
        punctuation = set(string.punctuation)

        skills_in_model = [s for s in tech_skills if s.lower() in model]

        all_stop_words = stop_words_en | stop_words_ru | punctuation

        vacancy_skills = []
        for row in vacancy_requirements:
            vacancy_skills += get_similar_skills(
                row["vacancy_id"],
                row["requirement"],
                model,
                skills_in_model,
                tech_skills,
                all_stop_words
            )

        vacancy_skills_df = pd.DataFrame(vacancy_skills, columns=vacancy_skills_column_names)

        vacancy_skills_file = makefile("processed", "vacancy_skills.csv")

        vacancy_skills_df.to_csv(vacancy_skills_file, index=False)

        return vacancy_skills_file


    # [Извлечение]
    @task_group(group_id="extract")
    def extract():
        extract_vacancies_from_api_task = extract_vacancies_from_api()
        fetch_currency_rates_task = fetch_currency_rates()


    # [Нормализация]
    @task_group(group_id="normalize")
    def normalize():
        normalize_vacancies_task = normalize_vacancies()
        normalize_employers_task = normalize_employers()
        normalize_vacancy_role_task = normalize_vacancy_role()
        normalize_vacancy_work_formats_task = normalize_vacancy_work_formats()


    # [Очистка null]
    @task_group(group_id="clean")
    def clean():
        drop_invalid_salaries_task = drop_invalid_salaries()
        fill_null_areas_task = fill_null_areas()
        check_new_coordinates_task = check_new_coordinates()

        add_new_coordinates_to_db_task = SQLExecuteQueryOperator(
            task_id="add_new_coordinates_to_db",
            conn_id="vacancy_db",
            sql="{{ ti.xcom_pull(task_id='fill_null_areas_and_generate_sql')['sql_path'] }}"
        )

        skip_add_coordinates = EmptyOperator(task_id="skip_add_coordinates")

        drop_invalid_salaries_task >> fill_null_areas_task
        fill_null_areas_task >> check_new_coordinates_task
        check_new_coordinates_task >> [add_new_coordinates_to_db_task, skip_add_coordinates]


    # [Парсинг скилов]
    @task_group(group_id="skills_parsing")
    def skills_parsing():
        parsing_skills_from_habr_task = parsing_skills_from_habr()
        parsing_skills_from_getmatch_task = parsing_skills_from_getmatch()
        extract_description_and_skills_from_hhru_task = extract_description_and_skills_from_hhru()


    # [Добавление скиллов]
    @task_group(group_id="update_reference")
    def update_reference():
        union_parsed_skills_and_generate_sql_task = union_parsed_skills_and_generate_sql()

        check_new_skills_task = check_new_skills()
    
        skip_add_skills = EmptyOperator(task_id="skip_add_skills")
            
        add_new_skills_to_db_task = SQLExecuteQueryOperator(
            task_id="add_new_skills_to_db",
            conn_id="vacancy_db",
            sql="{{ ti.xcom_pull(task_id='union_parsed_skills_and_generate_sql') }}"
        )

        union_parsed_skills_and_generate_sql_task >> check_new_skills_task
        check_new_skills_task >> [add_new_skills_to_db_task, skip_add_skills]


    # [Выделение скиллов из описаний]
    @task_group(group_id="analyze")
    def analyze():
        get_skills_from_requirements_task = get_skills_from_requirements()

    
    start = EmptyOperator(task_id="start")
    extract_group = extract()
    normalize_group = normalize()
    clean_group = clean()
    parsing_group = skills_parsing()
    update_group = update_reference()
    analyze_group = analyze()
    end = EmptyOperator(task_id="end")

    start >> extract_group >> normalize_group >> clean_group >> parsing_group >> update_group >> analyze_group >> end

