## Dependencies 
-------------- All Dependencies---------------

- Python 2.7 (Anaconda distro) x64 ubuntu

	- psycopg2  		2.6.1 
	- postgresql-9.4 
	- sqlalchemy
	- Pandas 			0.17.1
	- flask 			0.10.1
	- flask-sqlalchemy 	2.1
	- plotly 			1.9.6
	- nltk 				3.1
	- shutil 
	- os
	- zip
	- json
	- ftplib
	- datetime 
	- zipfile
	- numpy
	- sys
	- requests
	- lxml
	- re

## How to Run Scripts:

-  import database dump located in project directory: 
		filename: oquantdatabase.sql

-  This is not necessary unless you want to download all the data all over again.
-  The scripts would do it otherwise without prompting.

-   cd hedgefundproject/serverapp/data/

RUN:	python download_and_analyse.py
RUN:	python cikparser_dbloader.py

-   cd hedgefundproject/serverapp/

RUN: 	python app.py

-	Open client:
		0.0.0.0/9999 from browser: 
		(Tested on Chrome)

### Postgres Credentials and Setup

- PostgreSql apt-get(s)

	$ sudo apt-get install postgresql-9.4 
	+ 
	contrib

	$ sudo apt-get install pgadmin3

$ sudo -u postgres psql postgres

	role : postgres

postgres# \password 
	
	password : hedgefund

$ sudo -u postgres createdb oquantdatabase

	database = oquantdatabase
	password : hedgefund
	port : 5342

			bash: (connection to oquantdatabase)

				$ psql 
				irtza=# \c oquantdatabase 


-------local machine database connection--------
user: irtza
password : hedgefund
database : oquantdatabase

		bash: (connection to oquantdatabase)
		
			$ psql 
			irtza=# \c oquantdatabase 

