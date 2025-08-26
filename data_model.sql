create table employer (
	id 						int primary key,
	name 					varchar(40),
	total_rating			numeric,
	reviews_count			int,
	accredited_it_employer 	boolean,
	trusted 				boolean,
	logo_url				text
);

create table vacancy (
	id 			  	 int primary key,
	area 		  	 int,
	latitude 	  	 numeric,
	longitude 	  	 numeric,
	archived 	  	 boolean,
	created_at 	  	 timestamp,
	published_at  	 timestamp,
	has_test 	  	 boolean,
	internship 	  	 boolean,
	salary_from   	 int,
	salary_to 	  	 int,
	salary_frequency varchar(30),
	salary_currency  varchar(5),
	company_id 	  	 int,
	experience 	  	 varchar(18),
	employment	  	 varchar(20),
	constraint vacancy_company_id_fk foreign key (company_id) references employer(id)
);

create table roles (
	id 	 int primary key,
	name varchar(45)
);

create table vacancy_roles (
	vacancy_id int,
	role_id    int,
	constraint vr_vacancy_id_fk foreign key (vacancy_id) references vacancy(id),
	constraint vr_role_id_fk foreign key (role_id) references roles(id)
);

create table vacancy_work_formats (
	vacancy_id int,
	work_format varchar(20),
	constraint vwf_vacancy_id_fk foreign key (vacancy_id) references vacancy(id)
);

create table skills (
	id 		 serial primary key,
	name 	 varchar(50),
	category varchar(25)
);

create table vacancy_skills (
	vacancy_id int,
	skill_id   serial,
	constraint vs_vacancy_id_fk foreign key (vacancy_id) references vacancy(id),
	constraint vs_skill_id_fk foreign key (skill_id) references skills(id)
);

-- Какие профессии будем смотреть?
insert into roles values
(156, 'BI-аналитик, аналитик данных'),
(160, 'DevOps-инженер'),
(10, 'Аналитик'),
(12, 'Арт-директор, креативный директор'),
(150, 'Бизнес-аналитик'),
(25, 'Гейм-дизайнер'),
(165, 'Дата-сайентист'),
(34, 'Дизайнер, художник'),
(36, 'Директор по информационным технологиям (CIO)'),
(73, 'Менеджер продукта'),
(155, 'Методолог'),
(96, 'Программист, разработчик'),
(164, 'Продуктовый аналитик'),
(104, 'Руководитель группы разработки'),
(157, 'Руководитель отдела аналитики'),
(107, 'Руководитель проектов'),
(112, 'Сетевой инженер'),
(113, 'Системный администратор'),
(148, 'Системный аналитик'),
(114, 'Системный инженер'),
(116, 'Специалист по информационной безопасности'),
(121, 'Специалист технической поддержки'),
(124, 'Тестировщик'),
(125, 'Технический директор (CTO)'),
(126, 'Технический писатель');