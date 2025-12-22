US BLS Extract Transform Load Automation

This project automates the ETL process of working with US BLS data.

Background: US BLS provides geographic employment statistics data in multiple CSV files which is rich in insights and information.

Problem Statement: Working with these large datasets in CSV format is very cumbersome and limited. Excel is slow to load and process this data. If you want to view this geographical information in GIS visualization software on a national scale, it requires manually intensive data wrangling.

Solution: Use AI to wrangle the data in a Python script. Provide it the US BLS CSV file in the input folder, run csv2json_etl.py, and the output JSON data structure is created in the output folder.