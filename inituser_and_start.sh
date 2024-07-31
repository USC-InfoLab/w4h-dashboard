#!/bin/bash

#copy postgres conf files
cp -rf /app/conf/pg_hba.conf /etc/postgresql/15/main/pg_hba.conf
cp -rf /app/conf/postgresql.conf /etc/postgresql/15/main/postgresql.conf
cp -rf /app/conf/postgresql-42.7.3.jar /usr/share/java/postgresql-42.7.3.jar

#start postgresql
service postgresql start

#create admin account and database
#sudo -u postgres createuser -s -i -d -r -l -w admin
#sudo -u postgres psql -c "ALTER ROLE admin WITH PASSWORD 'admin';"
#sudo -u postgres psql -c "CREATE DATABASE admin;"
#sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE admin to admin;"
sudo -u postgres psql -f "/app/script/init.sql"

#create sample database
cd script
python -c "from loadSample import create_w4h_instance;create_w4h_instance('db','w4h-db')"
cd ..

echo "For documentation on using the W4H Toolkit see https://github.com/USC-InfoLab/w4h-documentation/blob/main/docs/index.md"

#start jupyter
jupyter notebook --NotebookApp.token='admin' --NotebookApp.password='admin' --ip 0.0.0.0 --port 8888 --allow-root --no-browser --ServerApp.root_dir='/app/notebooks/' --NotebookApp.allow_origin='https://colab.research.google.com' --NotebookApp.port_retries=0  &
# start dashboard
python init_user.py &
python stream_sim.py &
streamlit run viz.py --browser.serverAddress localhost


