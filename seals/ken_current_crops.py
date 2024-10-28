# Import necessary libraries
import os
import numpy as np
from osgeo import gdal
from rasterstats import zonal_stats
import pygeoprocessing as pygeo
import csv
import geopandas as gpd
import pandas as pd

# Introduction:
# This script processes raster data related to land suitability and calculates available land metrics
# for Kenya based on specific parameter sets. It outputs the results as raster files and calculates 
# zonal statistics, which are then merged with geographical data from a GeoPackage. 
# The final outputs are saved as a GeoPackage and a CSV file.

# Define paths
user_dir = os.path.expanduser('~')
available_land_inputs_path = os.path.join(user_dir, 'Files', 'base_data', 'gtap_invest', 'available_land_inputs_kenya')
cur_dir = os.path.join(user_dir, os.sep.join(['Files', 'seals', 'projects', 'run_ken_max_land']))
kenya_vector_path = '/Users/mbraaksma/Library/CloudStorage/GoogleDrive-braak014@umn.edu/Shared drives/NatCapTEEMs/Projects/JRC Project/Data/borders/kenya_aez.gpkg'










# Ensure the output directory exists
os.makedirs(cur_dir, exist_ok=True)

# List of input raster file names (without the .tif extension and *_10s suffix)
input_files = [
    'nutrient_retention_index',
    'oxygen_availability_index',
    'rooting_conditions_index',
    'toxicity_index',
    'workability_index',
    'excess_salts_index',
    'nutrient_availability_index',
    'caloric_yield',
    'TRI',
    'crop_suitability',
    'base_year_lulc_seals',
    'ha_per_cell'
]

# Define parameter sets
parameters_dict = {
    0: [1, 1, 1, 1, 1, 1, 1, 160000000000, 5, 60],
    1: [2, 2, 2, 2, 2, 2, 2, 80000000000, 8, 50],
    2: [3, 3, 3, 3, 3, 3, 3, 40000000000, 10, 40],
    3: [4, 4, 4, 4, 4, 4, 4, 20000000000, 20, 30],
    4: [5, 5, 5, 5, 5, 5, 5, 10000000000, 30, 20],
    5: [6, 6, 6, 6, 6, 6, 6, 5000000000, 40, 10],
    6: [7, 7, 7, 7, 7, 7, 7, 1000000000, 10000, 1]
}

# Helper function to load TIF file as NumPy array
def load_tif_as_array(tif_path):
    dataset = gdal.Open(tif_path)
    if dataset is None:
        raise ValueError(f"Could not open {tif_path}")
    return dataset.ReadAsArray()

# Load the raster data into NumPy arrays
available_land_inputs = {}
for input_file in input_files:
    file_path = os.path.join(available_land_inputs_path, input_file + '.tif')
    available_land_inputs[input_file] = load_tif_as_array(file_path)

# Define the operation function
def available_land_op(parameters, *input_arrays):
    # Calculate output arrays
    arable_array = np.where(
        (input_arrays[0] <= parameters[0]) & 
        (input_arrays[1] <= parameters[1]) & 
        (input_arrays[2] <= parameters[2]) & 
        (input_arrays[3] <= parameters[3]) &
        (input_arrays[4] <= parameters[4]) & 
        (input_arrays[5] <= parameters[5]) & 
        (input_arrays[6] <= parameters[6]) &
        (input_arrays[7] > parameters[7]) & 
        (input_arrays[8] <= parameters[8]) & 
        (input_arrays[9] >= parameters[9]) & 
        (input_arrays[10] >= 2) & (input_arrays[10] <= 5), 1, 0
    ) * input_arrays[11]

    current_and_arable_array = np.where((input_arrays[10] == 2) | (arable_array > 0), input_arrays[11], 0)

    return arable_array, current_and_arable_array

# Function to save the output raster with the same projection and geotransform
def save_raster(output_file_path, array, reference_dataset):
    driver = gdal.GetDriverByName('GTiff')
    out_dataset = driver.Create(output_file_path, array.shape[1], array.shape[0], 1, gdal.GDT_Float32)
    
    # Set the geotransformation and projection from the reference dataset
    reference_dataset_info = pygeo.geoprocessing.get_raster_info(reference_dataset)
    out_dataset.SetGeoTransform(reference_dataset_info['geotransform'])
    out_dataset.SetProjection(reference_dataset_info['projection_wkt'])
    
    # Write the array to the output file
    out_dataset.GetRasterBand(1).WriteArray(array)
    out_dataset.FlushCache()

# Get the reference dataset from one of the inputs (e.g., 'base_year_lulc_seals')
reference_dataset = os.path.join(available_land_inputs_path, 'base_year_lulc_seals.tif')

# Function to save zonal statistics to CSV
def save_zonal_stats_to_csv(stats, output_csv_path):
    with open(output_csv_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=stats[0].keys())
        writer.writeheader()
        writer.writerows(stats)

# Loop over parameters and perform calculations
for param_index, parameters in parameters_dict.items():
    # Perform the operation for the current parameter set
    arable_array, current_and_arable_array = available_land_op(
        parameters,
        available_land_inputs['nutrient_retention_index'],
        available_land_inputs['oxygen_availability_index'],
        available_land_inputs['rooting_conditions_index'],
        available_land_inputs['toxicity_index'],
        available_land_inputs['workability_index'],
        available_land_inputs['excess_salts_index'],
        available_land_inputs['nutrient_availability_index'],
        available_land_inputs['caloric_yield'],
        available_land_inputs['TRI'],
        available_land_inputs['crop_suitability'],
        available_land_inputs['base_year_lulc_seals'],
        available_land_inputs['ha_per_cell']
    )
    
    # Generate output file paths
    output_raster_paths = [
        os.path.join(cur_dir, f"arable_def{param_index}.tif"),
        os.path.join(cur_dir, f"current_and_arable_def{param_index}.tif")
    ]

    # Output the result arrays as raster files
    for output_array, output_file_path in zip([arable_array, current_and_arable_array], output_raster_paths):
        if not os.path.exists(output_file_path):
            # Output the resulting array with the same projection and geotransform
            save_raster(output_file_path, output_array, reference_dataset)
            print(f'Raster calculation complete for definition {param_index}. Output saved to:', cur_dir)

    # Generate output file paths for CSV
    output_csv_paths = [
        os.path.join(cur_dir, f"arable_def{param_index}.csv"),
        os.path.join(cur_dir, f"current_and_arable_def{param_index}.csv")
    ]

    # Calculate and save zonal statistics
    for output_raster_path, output_csv_path in zip(output_raster_paths, output_csv_paths):
        if not os.path.exists(output_csv_path):
            stats = zonal_stats(kenya_vector_path, output_raster_path, stats='sum')
            save_zonal_stats_to_csv(stats, output_csv_path)
            print(f'Zonal stats complete for definition {param_index}. Output saved to:', cur_dir)

# Load the GeoPackage into a GeoDataFrame
kenya_gdf = gpd.read_file(kenya_vector_path)

# Initialize an empty DataFrame to hold the merged CSV data
merged_csv_df = pd.DataFrame()

# Loop through each CSV file in cur_dir
for csv_file in os.listdir(cur_dir):
    # Check if the file is a CSV and starts with 'arable' or 'current'
    if csv_file.endswith('.csv') and (csv_file.startswith('arable') or csv_file.startswith('current')):
        
        csv_path = os.path.join(cur_dir, csv_file)
        csv_name = os.path.splitext(csv_file)[0]  # Use the filename (without extension) as the column name
        
        # Read the CSV file and rename 'sum' column to the filename
        csv_df = pd.read_csv(csv_path, index_col=False)
        csv_df = csv_df.rename(columns={'sum': csv_name})
        
        # Merge this CSV data into the merged DataFrame using the index
        if merged_csv_df.empty:
            merged_csv_df = csv_df
        else:
            merged_csv_df = merged_csv_df.merge(csv_df, how='outer', left_index=True, right_index=True)

# Sort the columns of the merged DataFrame
merged_csv_df = merged_csv_df.reindex(sorted(merged_csv_df.columns), axis=1)

# Merge with GeoDataFrame
merged_gdf = kenya_gdf.merge(merged_csv_df, how='outer', left_index=True, right_index=True)

# Save the merged GeoDataFrame to a new GeoPackage
output_gpkg_path = os.path.join(cur_dir, 'max_land_kenya.gpkg')
merged_gdf.to_file(output_gpkg_path, driver='GPKG')

# Save the merged CSV DataFrame to a new CSV file
final_df_csv = pd.DataFrame(merged_gdf)
output_csv_path = os.path.join(cur_dir, 'max_land_kenya.csv')
final_df_csv.to_csv(output_csv_path, index=False)

# Drop columns for simplicity
final_df_csv_simple = final_df_csv[['MAPREGH', 'ADM1_EN', 'ADM1_PCODE', 'pyramid_id', 'current_and_arable_def4']]
output_csv_path = os.path.join(cur_dir, 'max_land_kenya_def4.csv')
final_df_csv_simple.to_csv(output_csv_path, index=False)

# Output message with paths for both files
print(f"Final output saved as:\n- GPKG: {output_gpkg_path}\n- CSV: {output_csv_path}")
