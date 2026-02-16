US BLS Extract Transform Load Automation

This project automates the ETL process of working with US BLS data.

Background: US BLS provides geographic employment statistics data in multiple CSV files which is rich in insights and information.

Problem Statement: Working with these large datasets in CSV format is very cumbersome and limited. Excel is slow to load and process this data. If you want to view this geographical information in GIS visualization software on a national scale, it requires manually intensive data wrangling.

Solution: Use AI to wrangle the data in a Python script. Provide it the US BLS CSV file in the input folder, run csv2json_etl.py, and the output JSON data structure is created in the output folder.

## Shapefile Processing (`shapebuild/build_geoshapes.py`)

This script generates the primary map file used for visualization (`input/map_data/map_shape.geojson`).

It performs the following steps:
1.  Reads the U.S. state and Core-Based Statistical Area (CBSA) shapefiles.
2.  Calculates the non-metropolitan areas by subtracting the CBSA shapes from the state shapes.
3.  Combines the original CBSA shapes with the newly calculated non-metropolitan shapes into a single file.
4.  Assigns an `area_type` property ('CBSA' or 'Non-CBSA') to each polygon for easy identification.
5.  Simplifies the polygon geometries to reduce file size for better web performance.

To run the script, execute:
`python shapebuild/build_geoshapes.py`
