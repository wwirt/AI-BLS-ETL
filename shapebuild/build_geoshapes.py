# This Python code reads the Census CBSA shapefile and State shapefile to build the shape for the national map.
# It uses the GeoPandas library to read the shapefiles and perform spatial operations to create a unified shape for the national map.
# The two shapes are merged together to create a single shape, removes the overlaps, and is simplified to reduce the complexity of the shape for better performance when rendering the map.
# Finally, the resulting shape is saved as a GeoJSON file for use in the map visualization.

import geopandas as gpd
import os

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

# 2. Create a single unified geometry for all CBSAs and States
print("Unifying CBSA shapes...")
cbsa_unified = cbsa_gdf.unary_union
print("Unifying state shapes...")
state_unified = state_gdf.unary_union

# 3. Subtract the unified CBSA shape from the unified state shape
# This results in a geometry of areas that are in States but not in CBSAs.
print("Finding non-CBSA areas by subtracting CBSA shape from state shape...")
non_cbsa_areas = state_unified.difference(cbsa_unified)

# 4. Create a GeoDataFrame for the non-CBSA areas
non_cbsa_gdf = gpd.GeoDataFrame(index=[0], crs=cbsa_gdf.crs, geometry=[non_cbsa_areas])
non_cbsa_gdf['area_type'] = 'Non-CBSA'

# Add an 'area_type' column to the original CBSA dataframe
cbsa_gdf['area_type'] = 'CBSA'

# 5. Combine the original CBSA shapes with the new non-CBSA shapes
print("Combining CBSA and non-CBSA shapes...")
final_gdf = gpd.pd.concat([cbsa_gdf, non_cbsa_gdf], ignore_index=True)


# 6. Simplify the geometry to reduce file size and improve rendering performance
# The tolerance value may need adjustment.
print("Simplifying geometry...")
final_gdf['geometry'] = final_gdf.geometry.simplify(tolerance=0.01)

# 7. Save the final, processed result to GeoJSON
print(f"Saving final shape to {output_geojson_path}...")
final_gdf.to_file(output_geojson_path, driver='GeoJSON')

print("Processing complete.")