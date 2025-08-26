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
    skill_name VARCHAR(100),
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

CREATE TABLE dim_frequency (
    frequency_id SERIAL PRIMARY KEY,
    frequency_name VARCHAR(30)
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
    salary_frequency_id INT REFERENCES dim_frequency(frequency_id),
    experience_id INT REFERENCES dim_experience(experience_id),
    employment_id INT REFERENCES dim_employment(employer_id),
    has_test BOOLEAN,
    is_internship BOOLEAN
);

CREATE TABLE vacancy_status_history (
    history_id SERIAL PRIMARY KEY,
    vacancy_id INT REFERENCES fact_vacancy(vacancy_id),
    status BOOLEAN DEFAULT FALSE, -- archived or not
    change_reason TEXT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
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