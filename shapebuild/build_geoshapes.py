# This Python code reads the Census CBSA shapefile and State shapefile to build the shape for the national map.
# It uses the GeoPandas library to read the shapefiles and perform spatial operations to create a unified shape for the national map.
# The two shapes are merged together to create a single shape, removes the overlaps, and is simplified to reduce the complexity of the shape for better performance when rendering the map.
# Finally, the resulting shape is saved as a GeoJSON file for use in the map visualization.

import geopandas as gpd
import os
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths to your source shapefiles by joining the script's directory with the relative paths
cbsa_shp_path = os.path.join(script_dir, "shapefiles", "cb_2024_us_cbsa_20m.shp")
state_shp_path = os.path.join(script_dir, "shapefiles", "cb_2024_us_state_20m.shp")
output_geojson_path = os.path.join(script_dir, "national_map_shape.geojson")

# 1. Read the shapefiles directly into GeoDataFrames
print("Reading shapefiles...")
cbsa_gdf = gpd.read_file(cbsa_shp_path)
state_gdf = gpd.read_file(state_shp_path)

# Ensure the CRS (Coordinate Reference System) matches before any operation
if cbsa_gdf.crs != state_gdf.crs:
    print("CRS mismatch. Re-projecting state shapes to match CBSA shapes.")
    state_gdf = state_gdf.to_crs(cbsa_gdf.crs)

# 2. Use gpd.overlay() to subtract CBSA shapes from state shapes
# This is the most efficient way to perform this operation.
# It preserves state boundaries and attributes for the resulting non-CBSA areas.
print("Calculating non-CBSA areas using overlay...")
non_cbsa_gdf = gpd.overlay(state_gdf, cbsa_gdf, how='difference')
non_cbsa_gdf['area_type'] = 'Non-CBSA'

# 3. Prepare original CBSA data
cbsa_gdf['area_type'] = 'CBSA'

# 4. Combine the original CBSA shapes with the new non-CBSA shapes
print("Combining CBSA and non-CBSA shapes...")
final_gdf = pd.concat([cbsa_gdf, non_cbsa_gdf], ignore_index=True)


# 5. Simplify the geometry to reduce file size and improve rendering performance
# The tolerance value may need adjustment.
print("Simplifying geometry...")
final_gdf['geometry'] = final_gdf.geometry.simplify(tolerance=0.025)

# 6. Save the final, processed result to GeoJSON
print(f"Saving final shape to {output_geojson_path}...")
final_gdf.to_file(output_geojson_path, driver='GeoJSON')

print("Processing complete.")