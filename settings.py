from os import getcwd

# Если БД создана, то можно пропустить любую ветку, указав False
REFRESH_VACANCIES_AND_SKILLS = True
REFRESH_COMPANIES = True

# Путь результирующего файла
PATH_RESULT_JSON = f'{getcwd()}/top_key_skills.json'

# Можно установить нужную сферу
CODE_COMPANIES = "61"

# Имя соединения к базе данных в airflow
DB_CONN = 'sqllite_prof'
VACANCIES_TABLE_NAME = 'vacancies'
SKILLS_TABLE_NAME = 'skills'
COMPANIES_TABLE_NAME = 'companies'

# hh.ru
HH_API_URL = 'https://api.hh.ru/vacancies'
LIMIT = 100
VACANCY_API_PARAMS = {
    'per_page': 100,
    'text': 'python middle',
    'search_field': 'name',
    'host': 'hh.ru',
}

# egrul
URL_EGRUL = "https://ofdata.ru/open-data/download/egrul.json.zip"
PATH_EGRUL = f"{getcwd()}/egrul.json.zip"
