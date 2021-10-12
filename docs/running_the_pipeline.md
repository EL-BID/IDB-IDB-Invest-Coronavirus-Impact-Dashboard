# Running the Pipeline



## Build de Project 

To run the dashboard you need an instance. It can be small, for example, 4GB and two cores is enough.


1. Occupy memory in the instance

```
python ~/shared/spd-sdv-omitnik-waze/configs/occupy_memory.py
```

2. Makefile to create project directory and download repo

```
cd ~/shared/spd-sdv-omitnik-waze/configs
cat Makefile 
make REPO=IDB-IDB-Invest-Coronavirus-Impact-Dashboard
```

This will download the repo and build the environment by running `/start_env.sh`. If the repo doesn't have the shell file then it will just download it. 



3. Make cron

To schedule running the pipeline run the following

```
cd ~/projects/IDB-IDB-Invest-Coronavirus-Impact-Dashboard
make cron-tab
```

For more information related to cron scheduling go to section the section below, Run `crontab`.

4. View the log results

Finally, you can check the logged code at:
```
cd ~/projects/IDB-IDB-Invest-Coronavirus-Impact-Dashboard
cat log.log 
```

-  Run the pipeline independently 

To run the pipeline independently you can run the following shell code. 

```
cd ~/projects/IDB-IDB-Invest-Coronavirus-Impact-Dashboard
sh run_prod.sh 
```



----

**Important:** Make sure you have the correct configuration. 

You can check the s3_staging_dir at:
```
cd ~/shared/spd-sdv-omitnik-waze/corona/configs
cat athena.yaml 
vi athena.yaml 
```
To update the configuration for the project run setup secrets from the makefile.
```
cd ~/projects/IDB-IDB-Invest-Coronavirus-Impact-Dashboard
make setup-secrets
```


----

## Run `crontab`

Cron is a utility program for repeating tasks at a later time. Giving a command that schedules a task, at a specific time, repeatedly is a cron job.

A cron job is a time-based job scheduler in Unix-like computer operating systems.

1. To verify entries to the crontab.
```
crontab -l
```

2.  Scheduling job definition

Tasks scheduled in a crontab are structured like this:

```
minute hour day_of_month month day_of_week command_to_run
```

An example of job definition:
```
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) 
# |  |  |  |  |
# *  *  *  *  * user-name  command to be executed
```

For the dashboard, the code will run every day at 5 am. So the the schedule timer and the command are the following. You can find this at the Make file.

```
# write out current crontab
crontab -l > mycron
# echo new cron into cron file
echo "0 5 * * *      cd /home/$(USER)/projects/$(REPO); chmod +x run_prod.sh; ./run_prod.sh" >> mycron
# install new cron file
crontab mycron
rm mycron
```

This file will execute the file `run_prod.sh` that runs the pipeline. 


----

## Pipeline description


The objective of the pipeline is to process, according to specifications in a configuration file, the jams information by day and time for different regions (region_slug) of Latin America.


### Project Organization

    ├── configs                    <- Configuration files needed to run pipeline
    ├── data
    │   ├── output                 <- Output processed data
    │   ├── treated                <- The cleaned and treated 
    ├── docs                       <- Code documentation   
    ├── notebooks                  <- Jupyter notebooks
    ├── sql                        <- SQL files called to run pipeline
    ├── src                        <- Pipeline python files
    ├── tests                      <- Test module file
    ├── LICENSE                    <- Code license
    ├── Makefile                   <- Useful to build the env
    ├── README.md                  <- The top-level README for developers using this project (also known as this file!)
    ├── requirements.txt           <- Packages used in the code
    ├── run.sh                     <- Packages used in the code


- **`configs/`** this directory has three important elements:
  - `athena.yaml` paths and parameters requiered for the set up of Athena.
  - `config-template.yaml` this file saves important variables for the project, for example: pipelines to run, parameters to set up environment, athena parameters, local paths and default athena data base. To redirect to another config file modify `config_path`.
  - `dependency_graphs.yaml` this file contains the directed acyclic graph representing dependencies between runners in the pipeline. This file includes the pipeline name, runner to runner, name of the vertice and specific variables for each runner.  The variables from the dag have priority over variables from `config-template.yaml` file. 

- **`sql/`** this directory stores sql code used in the pipeline. Commonly, this code is executed with runners `basic_athena_query`.

- **`src/`** this directory contains python code to execute the pipeline. 
  - `core.py` includes function to run process. 
  - `entrypoint.py`
  - `logger.py`
  - `utils.py`
  - `\runners`: specific code that can be 
    - `basic_athena_query.py`
    - `create_athena_table.py`
    - `helpers.py`
    - `partitioned_athena_query.py`
    - `example_runner.py`





### Run single

To run the pipeline you need three parameters:

- `--dependency_graph_path`
- `--config_path` 
- `--slug`

These parameters are defined in the following code ran in a terminal with the environment defined by the project:

```
python src/entrypoint.py single --slug=raw --n_tries=1 --dependency_graph_path=configs/dependency-graph-raw.yaml --config_path=configs/config-raw.yaml
```

There are different slugs to run the pipeline. Each slug has a different directory in S3 where data created is stored. Also, the slug defines the name of the table name created in Athena. 

- `prod`: version running in master with cron tab. To be cautious with duplicated version
    - daily `current_millis: v12`
    - historical_2020 `current_millis: v13`
    
- `dev`: version with data updated up to July 2021 excluding country Mexico and country Brazil
    - daily `current_millis: v13`
    - historical_2020 `current_millis: v13`   
    
Testing ran under slug dev with the following parameter :    
    - test-v1 `current_millis: t1`
    - test-v2 `current_millis: t2`
    - test-v3 `current_millis: t3`



----

## Daily process data

```
- 
  path: 'sql/daily/create-filtered-table' # es la carpeta del código SQL a ejecutar
  runner: partitioned_athena_query        # es el código de python mediante el cual se ejecuta el código de SQL
  name: daily                             # nombre de la tabla en Athena
  verbose: False
  depends_on: [
    metadata_variation
  ]                                       # lista de nodos anteriores necesarios para ejecutar este paso
  current_millis: v13                     # version de almacenamiento (For more information go to Change daily data version  `current_millis`)
  mode: 'incremental'                     # forma de almacenar en Athena
  interval:
    start: 2021-07-11
    end: 2021-09-10                       # intervalo de fechas a procesar
```    


### Runner  `partitioned_athena_query.py`


In order to run paralel queries to populate the same table, the trick
is to create an empty table that points to a specific S3 path. Then,
a bunch of temporary tables are through an Athena process that points
to the same path. This populates the initial table that now can be
queried normally by Athena.

This also allows us to do that asynchronously. This function implements
that by using `multiprocessing.Pool` from python standard library.

First, it creates a list of dictionaries, `queries`, with two objects:

    - `make`: a query that creates the table
    - `drop`: a query that drops the same table

Then, this list is called by a Pool that has a number of jobs set by
the user in the config.yaml file and number_of_athena_jobs.


Process:


1. Create table if required

```
PARTITIONED BY (
    region_slug string
  )
  STORED AS ORC
  LOCATION '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}'
	 TBLPROPERTIES (
	  'classification'='orc', 
	  'compressionType'='zlib')
```

2. Partition query to run paralel queries to populate the same table. Generate query fills a string with jinja2 placeholders, {{ }}, with variables from the config.yaml file.

```
create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}_{{ p_name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/{{ p_path }}',
	  format='orc', orc_compression = 'ZLIB'
      )
``` 

3. Repair of table from partiition queries.

```
MSCK REPAIR TABLE  {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }};
```

#### Storage Organization


    ├── corona
    │   ├── dev    
    │   │   ├── v13    
    │   │   │   ├── daily 
    │   │   │   │   ├── daily 
    │   │   │   │   │   ├── region_slug=aguascalientes
    │   │   │   │   │   │   ├── year2020month03
    │   │   │   │   │   │   ├── year2020month04
    │   │   │   │   │   │   ├── year2020month05
    │   │   │   │   │   │   ├── year2020month06
    │   │   │   │   │   │   ├── year2021month07day01
    │   │   │   │   │   │   ├── year2021month07day02



### Change daily data version  `current_millis`


### Resource errors

El procesamiento de la información de jams consiste en leer la información y calcular la longitud de los congestionamientos o jams dentro de cada hora, día y región.  El pipeline recibe un archivo de configuración, sobre el que se detalla más adelante, y procesa las fechas y regiones para las que no hay información  disponible. Esta información se refleja en las tablas de Athena y en archivos csv.


Lectura de información de congestionamientos cada 5 minutos agregado por día, hora y región. Este paso ejecuta un query paralelizado por region_slug y día/semana/mes. 

Agregados diarios y semanales.

Creación de versiones de exportación privada y pública.
