# MS1 Data Export Tools

Scripts for exporting records from MS1 for migration into MS2. 

There are two scripts: `user_export.py` and `data_export.py`. These scripts have corresponding record output directories `user_export/` and `data_export/`.

MS1 user records have two variable-length dictionaries of user properties called `vars` and `volatile_vars`. These are exported as JSON files, with one JSON file per user. JSON files are identified by user ID in the filename. All other user records are located in a `ca_users.csv` file. 

Data records include facilities, institutions, media (groups), media files, projects, scanners, specimens, and taxonomies. Each of these record types has its own CSV file.

`user_export.py` should be run and its records should be migrated first, since data records reference user ID numbers. It will export all MS1 user records in the database. 

`data_export.py` must be given a list of media file IDs or project IDs to scrape data from. This will change in the future to export the entire MS1 database.
