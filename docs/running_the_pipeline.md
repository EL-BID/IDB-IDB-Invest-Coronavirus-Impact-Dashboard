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