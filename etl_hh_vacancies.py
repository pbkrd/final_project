import logging

from collections import defaultdict

import pandas as pd
import requests as rq

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from settings import HH_API_URL, VACANCY_API_PARAMS, LIMIT, VACANCIES_TABLE_NAME, SKILLS_TABLE_NAME, DB_CONN


def parse_vacancy_by_id(vacancy_id, storage):
    """
    Извлекает и парсит данные о вакансиях
    """
    logger = logging.getLogger(__name__)

    url = f'{HH_API_URL}/{vacancy_id}'
    vacancy_data = rq.get(url, params={'host': 'hh.ru'}).json()
    try:
        skills = vacancy_data['key_skills']
        if skills:
            data = []
            data.append(vacancy_data['name'])
            data.append(vacancy_data['employer']['name'])
            data.append(vacancy_data['description'])
            data.append([skill['name'].lower() for skill in skills])
            logger.debug(f'Вакансия {vacancy_id} валидна')

            fields = ['title', 'company_name', 'description', 'skills']
            [storage[field].append(value) for field, value in zip(fields, data)]
        logger.debug(f'Вакансия {vacancy_id} невалидна')
    except KeyError:
        logger.debug('Ошибка, поймана CAPTCHA')


def get_vacancies_data():
    """
    Извлекает и парсит данные о вакансиях
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    storage = defaultdict(list)
    session = rq.Session()
    for page in range(100):
        params = VACANCY_API_PARAMS | {'page': page}
        vacancies = session.get(HH_API_URL, params=params).json().get('items')

        try:
            [parse_vacancy_by_id(vacancy['id'], storage) for vacancy in vacancies]

            if 'title' in storage and len(storage['title']) >= LIMIT:
                break
        except Exception as err:
            logger.error(f"Ошибка: {err} при парсинге вакансий")
            break

    logger.info(f"Конец работы {__name__}")
    return storage


def dump_vacancies_to_db(**context):
    """
    Загружает данные о вакансиях в таблицы Vacancies и Skills
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    data = context['task_instance'].xcom_pull(task_ids='task_parse_vacancies_data')
    df = pd.DataFrame(data)
    # Добавление data в таблицу "Vacancies"
    vacancies = pd.DataFrame({'title': df['title'],
                              'company_name': df['company_name'],
                              'description': df['description']})

    # Добавление data в таблицу "Skills"
    skills = df.explode('skills').reset_index(drop=True)
    skills['skill_id'] = skills.groupby('skills').ngroup()
    skills = skills[['skill_id', 'skills']]
    skills = skills.drop_duplicates()

    # Создание связи "один ко многим" между таблицами "Vacancies" и "Skills"
    vacancies['skill_id'] = df['skills'].apply(
        lambda x: [skills[skills['skills'] == skill]['skill_id'].values[0] for skill in x])
    vacancies = vacancies.explode('skill_id')

    # Записываем в БД
    try:
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()

        vacancies.to_sql(VACANCIES_TABLE_NAME, conn, if_exists='append', index=False)
        skills.to_sql(SKILLS_TABLE_NAME, conn, if_exists='append', index=False)

        logger.info(f'Загружено {len(df)} вакансий')

    except Exception as err:
        logger.error(f"Не удалось записать результаты фильтрации в таблицу 'companies'. Ошибка: {err}")
        raise
    logger.info(f"Конец работы {__name__}")