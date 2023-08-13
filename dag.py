import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from elt_egrul import etl_egrul
from etl_hh_vacancies import get_vacancies_data, dump_vacancies_to_db
from settings import REFRESH_VACANCIES_AND_SKILLS, REFRESH_COMPANIES, PATH_EGRUL, VACANCIES_TABLE_NAME, \
    SKILLS_TABLE_NAME, COMPANIES_TABLE_NAME, URL_EGRUL, DB_CONN, PATH_RESULT_JSON


def check_need_refresh_vacancies():
    """
    Возвращает состояние настройки REFRESH_VACANCIES_AND_SKILLS - о необходимости обновления таблиц vacancies и skills
    """
    return REFRESH_VACANCIES_AND_SKILLS


def check_need_refresh_companies():
    """
    Возвращает состояние настройки REFRESH_COMPANIES - о необходимости обновления таблицы companies
    """
    return REFRESH_COMPANIES


def download_egrul_as_zip():
    """
    Функция загружает файл ЕГРЮЛ, в соответствии с параметрами URL_EGRUL, PATH_EGRUL
    """
    import urllib.request

    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    # Пробуем скачать файл
    try:
        urllib.request.urlretrieve(URL_EGRUL, PATH_EGRUL)
        logger.debug(f"Загрузка файла в {PATH_EGRUL} завершена")
    except OSError as err:
        logger.error(f"Не удалось скачать файл. Ошибка: {err}")
        raise

    logger.info(f"Конец работы {__name__}")


def create_table_skills():
    """
    Функция создает таблицу skills
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    try:
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()
        cursor = conn.cursor()

        conn.execute(f'DROP TABLE IF EXISTS {SKILLS_TABLE_NAME}')
        logger.info(f'Таблица {SKILLS_TABLE_NAME} успешно удалена!')

        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {SKILLS_TABLE_NAME} (
                           skill_id INT PRIMARY KEY AUTO_INCREMENT,
                           skill VARCHAR(255)
                        )''')
        logger.info(f'Таблица {SKILLS_TABLE_NAME} успешно добавлена!')

    except Exception as error:
        logger.error(
            f'Не удалось подключиться к базе данных (табл. {SKILLS_TABLE_NAME})'
            f'Исключение: {type(error)}: {error}',
        )
    logger.info(f"Конец работы {__name__}")


def create_table_vacancies():
    """
    Функция создает таблицу vacancies
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    try:
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()
        cursor = conn.cursor()

        conn.execute(f'DROP TABLE IF EXISTS {VACANCIES_TABLE_NAME}')
        logger.info(f'Таблица {VACANCIES_TABLE_NAME} успешно удалена!')

        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {VACANCIES_TABLE_NAME} (
                            title TEXT,
                            company_name TEXT,
                            description TEXT, 
                            FOREIGN KEY (skill_id) REFERENCES {SKILLS_TABLE_NAME}(skill_id)
                        )''')
        logger.info(f'Таблица {VACANCIES_TABLE_NAME} успешно добавлена!')

    except Exception as error:
        logger.error(
            f'Не удалось подключиться к базе данных (табл. {VACANCIES_TABLE_NAME})'
            f'Исключение: {type(error)}: {error}',
        )
    logger.info(f"Конец работы {__name__}")


def create_table_companies():
    """
    Функция создает таблицу companies
    """

    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    try:
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()
        cursor = conn.cursor()

        conn.execute(f'DROP TABLE IF EXISTS {COMPANIES_TABLE_NAME}')
        logger.info(f'Таблица {VACANCIES_TABLE_NAME} успешно удалена!')

        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {COMPANIES_TABLE_NAME}(
                           inn INTEGER,
                           title TEXT,
                           code_okved VARCHAR(15),
                           okved TEXT
                        )''')
        logger.info(f'Таблица {COMPANIES_TABLE_NAME} успешно добавлена!')

    except Exception as error:
        logger.error(
            f'Не удалось подключиться к базе данных (табл. {COMPANIES_TABLE_NAME})'
            f'Исключение: {type(error)}: {error}',
        )
    logger.info(f"Конец работы {__name__}")


def get_top_skills():
    """
    Функция сравнивающая компании из ЕГРЮЛ и из вакансий, затем выдывая топ ключевых навыков из вакансий, чьи компании вошли в отфильтрованный список
    """
    import pandas as pd

    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    # Создаем датафреймы из таблиц
    try:
        logger.debug(f"Читаем таблицы БД в датафреймы")
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()

        df_company = pd.read_sql_query("SELECT * FROM companies", conn)
        df_vacancies = pd.read_sql_query("SELECT * FROM vacancies AS V"
                                         "JOIN SKILLS_TABLE AS S"
                                         " ON V.skill_id = S.skill_id", conn)

        # Очищаем от правовых форм и запятых (в вакансиях)
        logger.debug(f"Очищаем имена компаний от правовых форм и считаем топ")
        repl_list = '|'.join(['ООО ', 'АО ', 'ПАО '])
        df_vacancies['company_name'] = \
            df_vacancies['company_name'].str.replace(repl_list, '', regex=True, case=False).str.split(r',').str[0]
        df_company['name'] = df_company['name'].str.split(r'"').str[1]

        # Ищем совпадения между названиями компаний
        df_vacancies = df_vacancies.assign(exst=df_vacancies['company_name'].isin(df_company['name']).astype(int))
        df_filtred_vac = df_vacancies.loc[df_vacancies['exst'] == 1]

        # Считаем топ и записываем
        grouped = df_filtred_vac.groupby('skill_id').size().reset_index(name='count')
        sorted_df = grouped.sort_values(by='count', ascending=False)
        top_10 = sorted_df.head(10)
        top_10.to_json(PATH_RESULT_JSON, orient='records')

    except Exception as err:
        logger.error(f"Не удалось записать результаты анализа топ ключевых навыков. Ошибка: {err}")
        raise

    logger.info(f"Конец работы {__name__}")


# DAG
default_args = {
    'owner': 'baryshev'
}

with DAG(
        dag_id='topskills',
        default_args=default_args,
        description='DAG for determining top-10 skills',
        start_date=datetime(2023, 8, 13, 8),
        schedule_interval=None
) as dag:
    check_vacancies = ShortCircuitOperator(
        task_id='task_check_need_refresh_vacancies',
        python_callable=check_need_refresh_vacancies,
        ignore_downstream_trigger_rules=False
    )

    check_companies = ShortCircuitOperator(
        task_id='task_check_need_refresh_companies',
        python_callable=check_need_refresh_companies,
        ignore_downstream_trigger_rules=False
    )

    table_skills = PythonOperator(
        task_id='task_create_table_skills',
        python_callable=create_table_skills
    )

    table_vacancies = PythonOperator(
        task_id='task_create_table_vacancies',
        python_callable=create_table_vacancies
    )

    table_companies = PythonOperator(
        task_id='task_create_table_companies',
        python_callable=create_table_companies
    )

    extract_vacancies = PythonOperator(
        task_id='task_parse_vacancies_data',
        python_callable=get_vacancies_data
    )

    upload_vacancies = PythonOperator(
        task_id='task_upload_vacancies_data_to_db',
        python_callable=dump_vacancies_to_db
    )

    download_egrul = PythonOperator(
        task_id='task_download_egrul',
        python_callable=download_egrul_as_zip
    )

    handling_egrul = PythonOperator(
        task_id='task_handling_egrul',
        python_callable=etl_egrul
    )

    top_skills = PythonOperator(
        task_id='task_get_top_skills',
        python_callable=get_top_skills,
        trigger_rule='none_failed'
    )

    check_vacancies >> [table_skills, table_vacancies, extract_vacancies] >> upload_vacancies >> top_skills
    check_companies >> [download_egrul, table_companies] >> handling_egrul >> top_skills
