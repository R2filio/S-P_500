# S-P_500
Procesamiento de datos end to end

En el siguiente repositorio se encuentran los archivos suficientes para el proceso end to end 
de la ingesta y analisis de los archivos historicos de acciones para S&P 500.


## 1.- Instalacion de Airflow
El airflow se monta sobre un contenedor docker, para llevar a cabo esto, es necesario correr el archivo
docker-compose, en una carpeta llamada airflow.

## 2.- Carga de esquema en Snowflake
El esquema de nombre esquema_s&p.sql se carga en una cuenta de snowflake es cual consta de dos tablas mostradas en
el diagrama entidad relacion JPEG tambien contenido en el repo

## 3.- Carga de DAG y archivo.
A continuacion se copia el archivo s&p_dag.py en la carpeta dag contenida en la carpeta airflow y el csv all_stocks_5yr.csv

## 4.- Correr el Dag

A continuacion se corre el dag y este importa el archivo, lo limpia, le agrega campos de auditoria,
lo separa en el formato correspondiente para el esquema de base de datos de snowflake, y carga los datos,
agregando llaves primarias y una foranea que relaciona a las tablas companies y stocks.
Ademas de generar un dataset, para continuar con la analitica.

## 5.- Analitica.

A continuacion se puede correr el archivo analisis.py el cual generara algunas estadisticas descriptivas, una prediccion
del cierre de precios por un modelo ML, una grafica de la evolucion del cierre para una empresa, asi como algunas otras graficas de interes.

## 6.- Power BI

Los analiss anteriores tambien se pueden generar en Power bi en el siguiente tablero publicado

