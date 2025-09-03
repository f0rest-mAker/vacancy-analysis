CREATE TABLE dim_employer (
    employer_id INT PRIMARY KEY,
    name VARCHAR(100),
    total_rating NUMERIC NULL,
    reviews_count INT NULL,
    accredited_it_employer BOOLEAN,
    trusted BOOLEAN,
    logo_url TEXT
);

CREATE TABLE staging_employer AS TABLE dim_employer WITH NO DATA;

CREATE TABLE dim_role (
    role_id INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE dim_area (
    area_id SERIAL PRIMARY KEY,
    area_name VARCHAR(100),
    latitude NUMERIC,
	longitude NUMERIC
);

CREATE TABLE dim_skill (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100)
);

CREATE TABLE dim_work_format (
    work_format_id SERIAL PRIMARY KEY,
    format_name VARCHAR(50)
);

CREATE TABLE dim_experience (
    experience_id SERIAL PRIMARY KEY,
    experience_name VARCHAR(18)
)

CREATE TABLE dim_employment (
    employment_id SERIAL PRIMARY KEY,
    employment_name VARCHAR(20)
)

CREATE TABLE fact_vacancy (
    vacancy_id INT PRIMARY KEY,
    employer_id INT REFERENCES dim_employer(employer_id),
    area_id INT REFERENCES dim_area(area_id),
    role_id INT REFERENCES dim_role(role_id),
    published_date DATE,
    created_date DATE,
    salary_from INT NULL,
    salary_to INT NULL,
    experience_id INT REFERENCES dim_experience(experience_id),
    employment_id INT REFERENCES dim_employment(employment_id),
    has_test BOOLEAN,
    is_internship BOOLEAN
);

CREATE TABLE vacancy_status_history (
    history_id SERIAL PRIMARY KEY,
    vacancy_id INT REFERENCES fact_vacancy(vacancy_id),
    status BOOLEAN DEFAULT FALSE, -- archived or not
    change_reason TEXT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL
);

CREATE TABLE bridge_vacancy_skill (
    vacancy_id INT REFERENCES fact_vacancy(vacancy_id),
    skill_id INT REFERENCES dim_skill(skill_id),
    PRIMARY KEY(vacancy_id, skill_id)
);

CREATE TABLE bridge_vacancy_work_format (
	vacancy_id INT REFERENCES fact_vacancy(vacancy_id),
	work_format_id INT REFERENCES dim_work_format(work_format_id),
	PRIMARY KEY(vacancy_id, work_format_id)
)

insert into dim_work_format (format_name) values
('На месте работодателя'),
('Удалённо'),
('Гибрид'),
('Разъездной');

insert into dim_experience (experience_name) values
('Нет опыта'),
('От 1 года до 3 лет'),
('От 3 до 6 лет'),
('Более 6 лет');

insert into dim_employment (employment_name) values
('Полная занятость'),
('Частичная занятость'),
('Проектная работа'),
('Волонтерство'),
('Стажировка');

insert into dim_role (role_id, name) values
(156,'BI-аналитик, аналитик данных'),
(160,'DevOps-инженер'),
(10,'Аналитик'),
(12,'Арт-директор, креативный директор'),
(150,'Бизнес-аналитик'),
(25,'Гейм-дизайнер'),
(165,'Дата-сайентист'),
(34,'Дизайнер, художник'),
(36,'Директор по информационным технологиям (CIO)'),
(73,'Менеджер продукта'),
(155,'Методолог'),
(96,'Программист, разработчик'),
(164,'Продуктовый аналитик'),
(104,'Руководитель группы разработки'),
(157,'Руководитель отдела аналитики'),
(107,'Руководитель проектов'),
(112,'Сетевой инженер'),
(113,'Системный администратор'),
(148,'Системный аналитик'),
(114,'Системный инженер'),
(116,'Специалист по информационной безопасности'),
(121,'Специалист технической поддержки'),
(124,'Тестировщик'),
(125,'Технический директор (CTO)'),
(126,'Технический писатель');