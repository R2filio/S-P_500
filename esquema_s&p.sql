CREATE TABLE companies (
    company_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

create or replace TABLE KIOSKO.ANALISIS_DE_DATOS.STOCKS (
	STOCK_ID VARCHAR(200) NOT NULL,
	COMPANY_ID VARCHAR(100),
	NAME VARCHAR(100),
	DATE DATE NOT NULL,
	OPEN NUMBER(10,2) NOT NULL,
	HIGH NUMBER(10,2) NOT NULL,
	LOW NUMBER(10,2) NOT NULL,
	CLOSE NUMBER(10,2) NOT NULL,
	VOLUME NUMBER(38,0) NOT NULL,
	CREATED_AT VARCHAR(100),
	CREATED_BY VARCHAR(100),
	primary key (STOCK_ID),
	foreign key (COMPANY_ID) references KIOSKO.ANALISIS_DE_DATOS.COMPANIES(COMPANY_ID)
);