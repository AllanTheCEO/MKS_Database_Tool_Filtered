# MKS_Database_Tool_Filtered

Centralized Excel data from distributed machines into a central database organized by SQL and Parquet.
A Python FastHTML API and UI allows users to filter the data and access hosted data processing modules.
Database and Applications (UI) side are stored on two seperate servers. A docker container hosts the web interface and allows the applications side to access the database.

 - Compressed 3TB+ of Excel Data for ~20 users
 - Two types: MAT and DVT data. Implemented MAT first as proof-of-concept.
 - Python watchdog uses polling to monitor directories and upload changes to staging_dir. A seperate system (not fully implemented by end of co-op) automatically ingests the staging_dir to the database.

![UI: Main Page](assets/Main_Page.png)
The main page allows for filtering between different data types, test types, test stands, and units. It will automatically display valid matches in columns below, but this will not be shown here for security.

![UI: Modules Page](assets/Modules_Page.png)
The modules page allows for accessing data processing modules, usually jupyter or marimo notebooks. The idea is that people can build their own data analysis tools and host them on the server for other people to use. The current modules availible are ones that I build for different analysis tools. These were also long-term projects themselves. All the tools will be hosted on the applications server to avoid fixing issues on a machine-by-machine basis. Eventually, the goal is to set a pipeline directly from the database server into individual data modules.

![High-Level System Diagram](assets/System_Diagram.png)
 - Two-tier parquet system: Bronze and Silver. A silver parquet file represents a serial number folder. A bronze parquet file represents an excel file. A silver parquet file contains a list of bronze file paths and unit-level data. I chose this design because a typical query is done by serial number, and this allows the user to only access the files of a certain serial number or get unit-level metadata without accessing the bronze parquet files; this is the most efficient system in most cases.
 - Read SQL, not Parquet files. Traversing a SQL database is very fast compared to opening and reading thousands of parquet files. When filtering data, we want to access the SQL database whenever possible. This means the SQL database also contains metadata ingested from the parquet files. During testing, this change significantly improved query time with lots of data. 
 - LRU Cache for repeated serial number queries.
 - Ingestion process: Test stand -> parquet; program reads excel files to determine metadata, parquet -> SQL (dbo.Bronze, dbo.Silver). Python watchdog automates subsequent data ingestion from test stands. Most importantly, the process is IDEMPOTENT, meaning multiple ingestion commands have the same result. This by itself eliminates most data ingestion errors.
 - Docker container coordinates UI, database, and data processing modules.

![Docker Logs](assets/Docker_Logs.png)
![app.py Logs](assets/app.py_Logs.png)