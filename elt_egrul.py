import json
import logging
import zipfile

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from settings import COMPANIES_TABLE_NAME, CODE_COMPANIES, PATH_EGRUL, DB_CONN


def etl_egrul():
    logger = logging.getLogger(__name__)
    logger.info(f"Начало работы {__name__}")

    try:
        sqlite_hook = SqliteHook(sqlite_conn_id=DB_CONN)
        conn = sqlite_hook.get_conn()
        cursor = conn.cursor()

        try:
            with zipfile.ZipFile(PATH_EGRUL, "r") as zip_obj:
                filenames = zip_obj.namelist()
                for filename in filenames:
                    # Парсинг json
                    with zip_obj.open(filename) as file:
                        companies = json.load(file)
                        for company in companies:
                            try:
                                props_okved = company["data"]["СвОКВЭД"]["СвОКВЭДОсн"]
                                code_okved = props_okved["КодОКВЭД"]
                                if code_okved.startswith(CODE_COMPANIES):
                                    inn = company["inn"]
                                    title = company["name"]
                                    okved = props_okved["НаимОКВЭД"]
                                else:
                                    raise ValueError
                            except (KeyError, ValueError):
                                pass
                            else:
                                # Выгрузка данных в БД
                                insert_data = f"""
                                                  INSERT INTO {COMPANIES_TABLE_NAME} (inn, title, code_okved, okved)
                                                  VALUES (?, ?, ?, ?, ?)
                                                  """
                                values = (inn, title, code_okved, okved)

                                cursor.execute(insert_data, values)
                                conn.commit()
        except zipfile.BadZipFile as error:
            logger.error(f"Файл ЕГРЮЛ по пути {PATH_EGRUL} не(до)загружен. Ошибка: {error}")
            raise

    except Exception as error:
        logging.error(
            f'Не удалось подключиться к базе данных (табл. {COMPANIES_TABLE_NAME})\n'
            f'Исключение: {type(error)}: {error}',
        )
    logger.info(f"Конец работы {__name__}")