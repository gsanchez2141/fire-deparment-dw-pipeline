# fire-deparment-dw-pipeline

![img.png](Architecture%20Diagram%2Fimg.png)

# Introduction
End to end pipeline loading a Data Warehouse according to fire department data

# Technologies
Mockup Generator
- Polars
Main Job
- Pandas

# Steps: 

1. This requires to have an ~/.aws/credentials file for localstack to work
- Open your terminal
- `cd ~`
- `mkdir -p .aws`
- Use a text editor (e.g., nano, vim, or echo) to create and edit the ~/.aws/credentials file. Here, I'll use echo:
  - `echo -e "[default]\naws_access_key_id=sample-access-key-id\naws_secret_access_key=sample-access-key-id\nregion=us-east-1" > ~/.aws/credentials`
- `cat ~/.aws/credentials`

2. Install Docker
- Follow the installation instructions for your operating system on the [Docker website](https://docs.docker.com/engine/install/).
3. Install LocalStack AWS CLI:
- Follow the installation instructions for your operating system on the [awscli-local website](https://github.com/localstack/awscli-local).
4. Download the repo
- Open your terminal, go to your favorite folder
- `git clone https://github.com/gsanchez2141/fire-deparment-dw-pipeline.git`
5. Once inside the root of the repo
- `cd bin`
- `./docker-script.sh start` - this will start the docker compose with postgres with all the required tables and localstack additionally it will create a bucket on s3 and load the file sample from the fire department   
- `./first-run.sh start` - this will run the main job, persist the data from the raw file on to the DW
- `./s3_generator.sh start` -  this will run the mockup generator, generate mockup incremental data, it will append the mockup data to the raw file guarantying the row uniqueness according to `'incident_number', 'id', 'call_number'`, push the new file back to s3 with a new file name. Afterward it will run the main job again and load the new file on to the DW   
6. Log in on to the database to run the sample queries and validate the data
- With your favorite IDE and with the following credentials
- db_host = localhost 
- db_port = 5432
- db_name = mydatabase
- db_user = myuser
- db_password = mypassword
- Sample queries: [analytical_queries.sql](analytical_queries.sql)
7.`./s3_generator.sh start` can be ran N amount of times
- run the following to validate that the muckup data is being run and loaded in an incremental manner 
```
select *
from fact_fire_department_injuries
order by 1  desc
limit 10
```
```
select count(*)
from fact_fire_department_injuries
```
8. To shut down and destroy everything run
- `./s3_generator.sh stop`
9. Process can be restarted again from step 5, and start from scratch as many times as needed
