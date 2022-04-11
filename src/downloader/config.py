import os


CLICKHOUSE_PARAMS = {
    'host': 'localhost',
    'user': os.getenv('HH_API_USERNAME'),
    'password': os.getenv('HH_API_PASSWORD'),
}

VACANCY_TYPES = {
    'id': 'int', # Идентификатор вакансии
    'premium': 'bool',
    'name': 'str', # название вакансии
    'department_id': 'str', # nullable
    'department_name': 'str', # nullable
    'has_test': 'bool', # Информация о наличии прикрепленного тестового задании к вакансии
    'response_letter_required': 'bool', # Обязательно ли заполнять сообщение при отклике на вакансию
    'area_id': 'int', # Идентификатор региона
    'area_name': 'str', # Название региона
    'area_url': 'str', # Url получения информации о регионе
    'salary_from': 'int', # Нижняя граница вилки оклада nullable
    'salary_to': 'int', # Верняя граница вилки оклада nullable
    'salary_gross': 'bool', # Признак того что оклад указан до вычета налогов nullable
    'salary_currency': 'str', # Идентификатор валюты оклада nullable
    'type_id': 'str', # Идентификатор типа вакансии
    'type_name': 'str', # Название типа вакансии
    'address_city': 'str', # nullable
    'address_street': 'str', # nullable
    'address_building': 'str', # nullable
    'address_description': 'str', # nullable
    'address_lat': 'float', # nullable
    'address_lng': 'float', # nullable
    'address_raw': 'str', # nullable
    'address_id': 'int', # nullable
    'address_metro_station_id': 'str', # nullable
    'address_metro_station_name': 'str', # nullable
    'address_metro_line_id': 'str', # nullable
    'address_metro_line_name': 'str', # nullable
    'address_metro_lat': 'float', # nullable
    'address_metro_lng': 'float', # nullable
    'response_url': 'str', # nullable
    'sort_point_distance': 'float', # nullable
    'published_at': 'str', # '%Y-%m-%dT%X%z'
    'created_at': 'str', #  '%Y-%m-%dT%X%z'
    'archived': 'bool', # Находится ли данная вакансия в архиве
    'apply_alternate_url': 'str', # Ссылка на отклик на вакансию на сайте
    'insider_interview_id': 'int', # nullable
    'insider_interview_url': 'str', # nullable Интервью о жизни в компании
    'alternate_url': 'str', # Ссылка на представление вакансии на сайте
    'employer_id': 'int',
    'employer_name': 'str',
    'employer_type': 'str', # nullable
    'employer_site_url': 'str', # nullable
    'employer_discription': 'str', # nullable
    'employer_url': 'str', # nullable
    'employer_branded_description': 'str', # nullable
    'employer_vacancies_url': 'str', # nullable
    'employer_open_vacancies': 'int', # nullable
    'employer_trusted': 'bool',
    'employer_alternate_url': 'str',
    'employer_insider_interviews_id': 'List[int]', # nullable
    'employer_insider_interviews_title': 'List[str]', # nullable
    'employer_insider_interviews_url': 'List[str]', # nullable
    'employer_logo_urls_original': 'str', # nullable
    'employer_logo_urls_240': 'str', # nullable
    'employer_logo_urls_90': 'str', # nullable
    'employer_area_id': 'int', # nullable
    'employer_area_name': 'str', # nullable
    'employer_area_url': 'str', # nullable
    'employer_relations': 'List[str]', # nullable
    'employer_blacklisted': 'bool',
    'snippet_requirement': 'str', # nullable
    'snippet_responsibility': 'str', # nullable
    'counters_responses': 'int', # nullable
    'contacts_name': 'str', # nullable
    'contacts_email': 'str', # nullable
    'contacts_phones_country': 'List[str]', # nullable
    'contacts_phones_city': 'List[str]', # nullable
    'contacts_phones_number': 'List[str]', # nullable
    'contacts_phones_comment': 'List[str]', # nullable
    'schedule_id': 'str',
    'schedule_name': 'str',
    'working_days_id': 'str', # nullable
    'accept_temporary': 'bool',
    'url': 'str',
}
