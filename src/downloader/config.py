import os


non_type = lambda x: x

def type_transformation(type_):
    def func(feature):
        if feature:
            return type_(feature)
        return feature
    return func

DATE_REGEX = '%Y-%m-%dT%X%z'

CLICKHOUSE_PARAMS = {
    'host': 'localhost',
    'user': os.getenv('HH_API_USERNAME'),
    'password': os.getenv('HH_API_PASSWORD'),
}

VACANCY_TYPES = {
    'id': type_transformation(int), # Идентификатор вакансии
    'premium': type_transformation(int),
    'name': type_transformation(str), # название вакансии
    'department_id': type_transformation(str), # nullable
    'department_name': type_transformation(str), # nullable
    'has_test': type_transformation(int), # Информация о наличии прикрепленного тестового задании к вакансии
    'response_letter_required': type_transformation(int), # Обязательно ли заполнять сообщение при отклике на вакансию
    'area_id': type_transformation(int), # Идентификатор региона
    'area_name': type_transformation(str), # Название региона
    'area_url': type_transformation(str), # Url получения информации о регионе
    'salary_from': type_transformation(int), # Нижняя граница вилки оклада nullable
    'salary_to': type_transformation(int), # Верняя граница вилки оклада nullable
    'salary_gross': type_transformation(int), # Признак того что оклад указан до вычета налогов nullable
    'salary_currency': type_transformation(str), # Идентификатор валюты оклада nullable
    'type_id': type_transformation(str), # Идентификатор типа вакансии
    'type_name': type_transformation(str), # Название типа вакансии
    'address_city': type_transformation(str), # nullable
    'address_street': type_transformation(str), # nullable
    'address_building': type_transformation(str), # nullable
    'address_description': type_transformation(str), # nullable
    'address_lat': type_transformation(float), # nullable
    'address_lng': type_transformation(float), # nullable
    'address_raw': type_transformation(str), # nullable
    'address_id': type_transformation(int), # nullable
    'address_metro_station_id': type_transformation(str), # nullable
    'address_metro_station_name': type_transformation(str), # nullable
    'address_metro_line_id': type_transformation(str), # nullable
    'address_metro_line_name': type_transformation(str), # nullable
    'address_metro_lat': type_transformation(float), # nullable
    'address_metro_lng': type_transformation(float), # nullable
    'response_url': type_transformation(str), # nullable
    'sort_point_distance': type_transformation(float), # nullable
    'published_at': type_transformation(str), # '%Y-%m-%dT%X%z'
    'created_at': type_transformation(str), #  '%Y-%m-%dT%X%z'
    'archived': type_transformation(int), # Находится ли данная вакансия в архиве
    'apply_alternate_url': type_transformation(str), # Ссылка на отклик на вакансию на сайте
    'insider_interview_id': type_transformation(int), # nullable
    'insider_interview_url': type_transformation(str), # nullable Интервью о жизни в компании
    'alternate_url': type_transformation(str), # Ссылка на представление вакансии на сайте
    'employer_id': type_transformation(int),
    'employer_name': type_transformation(str),
    'employer_type': type_transformation(str), # nullable
    'employer_site_url': type_transformation(str), # nullable
    'employer_discription': type_transformation(str), # nullable
    'employer_url': type_transformation(str), # nullable
    'employer_branded_description': type_transformation(str), # nullable
    'employer_vacancies_url': type_transformation(str), # nullable
    'employer_open_vacancies': type_transformation(int), # nullable
    'employer_trusted': type_transformation(int),
    'employer_alternate_url': type_transformation(str),
    # 'employer_insider_interviews_id': 'List[int]', # nullable
    # 'employer_insider_interviews_title': 'List[str]', # nullable
    # 'employer_insider_interviews_url': 'List[str]', # nullable
    'employer_logo_urls_original': type_transformation(str), # nullable
    'employer_logo_urls_240': type_transformation(str), # nullable
    'employer_logo_urls_90': type_transformation(str), # nullable
    'employer_area_id': type_transformation(int), # nullable
    'employer_area_name': type_transformation(str), # nullable
    'employer_area_url': type_transformation(str), # nullable
    # 'employer_relations': 'List[str]', # nullable
    'employer_blacklisted': type_transformation(int),
    'snippet_requirement': type_transformation(str), # nullable
    'snippet_responsibility': type_transformation(str), # nullable
    'counters_responses': type_transformation(int), # nullable
    'contacts_name': type_transformation(str), # nullable
    'contacts_email': type_transformation(str), # nullable
    # 'contacts_phones_country': 'List[str]', # nullable
    # 'contacts_phones_city': 'List[str]', # nullable
    # 'contacts_phones_number': 'List[str]', # nullable
    # 'contacts_phones_comment': 'List[str]', # nullable
    'schedule_id': type_transformation(str),
    'schedule_name': type_transformation(str),
    'working_days_id': type_transformation(str), # nullable
    'accept_temporary': type_transformation(int),
    'url': type_transformation(str),
}
