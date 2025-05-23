# gcloud_medaillon

## initializing project 

### pre-requisite

> in GCP : 
#### create bucket, folders and upload files :

demo_etl_data_ing
    ---crm
        ---crm_cust_info.csv
        ---crm_prd_info.csv
        ---crm_sales_details.csv
    ---erp
        ---erp_cust_az12.csv
        ---erp_loc_a101.csv
        ---erp_px_cat_g1v2.csv


> in the python project : 
#### change names in the dag file (paths, projects, etc.):
- bronze dataset id (ex: my-project-demo-dwh.dwh_bronze)
- silver dataset id
- gold dataset id
- bucket id (ex: **my-project-demo-dwh**)
- client id (storage@project...) (service principal) (ex : sa-bq-demo-dwh@my-project-demo-dwh.iam.gserviceaccount.com)

> in GCP :
#### give client id access to the following iam roles :
- Administrateur BigQuery
- Administrateur de compte de service
- Administrateur de dossier Storage
- Créateur de jetons du compte de service

> in the python Dockerfile : 
#### by default the dockerfile is filled with this (delete this line) :

````
FROM astrocrpublic.azurecr.io/runtime:3.0-2
````

#### fill in the dockerfile with the following : 

```
FROM quay.io/astronomer/astro-runtime:12.8.0

ENV ENV_PATH="/usr/local/airflow/env"
ENV PATH="$ENV_PATH/bin:$PATH"

RUN python -m venv $ENV_PATH && \
    source $ENV_PATH/bin/activate && \
    pip install apache-airflow-providers-google==15.1.0 --no-deps

COPY /gcp/k.json /usr/local/airflow/gcp/k.json
```

#### go in the GCP cloud shell (editor mode) copy files from this project (dwh_project_220525) including the following :

dags/
- dags/dag_airflow_dwh.py (all your dags from bronze to gold)

gcp/
- gcp/k.json (your key in json format)

sql/
- sql/bronze/ddl_bronze_table.sql
- sql/silver/ddl_silver_table.sql
- sql/silver/dml_silver_table.sql
- sql/gold/dml_gold_table_dim.sql
- sql/gold/dml_gold_table_facts.sql


#### go in GCP cloud shell, make a virtualenv, install astro, make directories

create and initialize a virtualenv in cloud shell (shell mode)
#### make a project directory
#### go to the root of the project (dwh_project_22052) and initialize astro : 

>check where you are
> ```pwd```

>create python virtalenv 
>```python -m venv test env```

>activate python virtalenv 
>```source env/bin/activate```

>make a project directory
```mkdir dwh_project_220525```

>go to the project directory
``cd dwh_project_220525``

> make a sql folder and medaillon folders 
``mkdir sql``
``cd sql``
``mkdir bronze``
``mkdir silver``
``mkdir gold``

> install astro app
``curl -sSL install.astronomer.io | sudo bash -s``

> initialize the astro project
``astro dev init``

> start the airflow local 
``astro dev start``


BONUS CMD : 
> to restart the container if needed (if you changed files in your astro project)
``astro dev restart``
> to kill your container
``astro dev kill`` 
> to remove your local docker container
``docker rm $(docker ps -aq)``

#### go to the local url provided by astro :
> looks like (click on the one is given):
http://localhost:8080
> log in as admin (pass : admin)

#### configure a new connection for airflow
> go to admin/connections
> add connection
> provide the following infos :
> - connection id : name you gave in the default var in ``GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var="bq-airflow")``
> - connection type : Google BigQuery
> - project id : <my-gcp-project-id> (ex : my-project-demo-dwh) 
> - keyfile path : (ex : /usr/local/airflow/gcp/k.json) as you specified it in the docker

#### launch you dag
> go to you dag (ex : dag_airflow_dwh.py)
> trigger dag
> wait for your jobs being done