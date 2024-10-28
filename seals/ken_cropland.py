import os
import pandas as pd
from osgeo import gdal
import pygeoprocessing as pygeo
from rasterstats import zonal_stats
import numpy as np

# Define user and project directories
user_dir = os.path.expanduser('~')
cur_dir = os.path.join(user_dir, 'Files', 'seals', 'projects', 'run_ken_max_land', 'cropland_seals')
os.makedirs(cur_dir, exist_ok=True)

# Define the path to the Kenya vector shapefile
kenya_vector_path = '/Users/mbraaksma/Library/CloudStorage/GoogleDrive-braak014@umn.edu/Shared drives/NatCapTEEMs/Projects/JRC Project/Data/borders/un-ocha/ken_adm_iebc_20191031_shp/ken_admbnda_adm0_iebc_20191031.shp'
kenya_aez_path = '/Users/mbraaksma/Files/base_data/demetra-seals/borders/kenya_aez.gpkg'

# Define the directory containing the time series of raster files
raster_dir = os.path.join(user_dir, 'Files', 'base_data', 'lulc', 'esa')

# Initialize a list to store zonal statistics for each time step
all_stats = []

# Loop through each raster file in the raster_dir
for raster_file in os.listdir(raster_dir):
    if raster_file.startswith('lulc_esa_') and raster_file.endswith('.tif'):
        raster_path = os.path.join(raster_dir, raster_file)
        
        # Define the output raster path for the clipped file
        output_raster_path = os.path.join(cur_dir, raster_file)
        
        # Check if the warped file already exists; if so, skip
        if os.path.exists(output_raster_path):
            print(f"Clipped file already exists, skipping: {output_raster_path}")
        else:
            # Perform warping on each raster file to crop it to the boundaries defined by the Kenya vector
            gdal.Warp(output_raster_path,
                      raster_path,
                      cutlineDSName=kenya_vector_path,
                      cropToCutline=True,
                      srcNodata=pygeo.geoprocessing.get_raster_info(raster_path)['nodata'][0],
                      dstNodata=pygeo.geoprocessing.get_raster_info(raster_path)['nodata'][0])
            print(f"Clipping completed: {output_raster_path}")
        
        # Extract the year from the filename (e.g., 'lulc_esa_1992.tif' -> 1992)
        year = raster_file.split('_')[-1].split('.')[0]

        # Read the clipped raster to create a new raster based ha_per_cell
        raster_dataset = gdal.Open(output_raster_path)
        raster_array = raster_dataset.GetRasterBand(1).ReadAsArray()

        # Read the ha_pre_cell file
        ha_per_cell_path = os.path.join(user_dir, 'Files', 'base_data', 'gtap_invest', 'available_land_inputs_kenya', 'ha_per_cell.tif')
        ha_per_cell_ds = gdal.Open(ha_per_cell_path)
        ha_per_cell_array = ha_per_cell_ds.GetRasterBand(1).ReadAsArray()

        # ha_per_cell in cropland pixels
        crop_array = np.where(
            (raster_array == 10) | (raster_array == 11) | (raster_array == 12) | 
            (raster_array == 20) | (raster_array == 30) | (raster_array == 40),
            ha_per_cell_array, 
            0
        )
        # Create a new raster file path
        new_raster_path = os.path.join(cur_dir, f"cropland_ha_per_cell_{year}.tif")

        # Create a new raster to save the new data
        driver = gdal.GetDriverByName('GTiff')
        new_raster_dataset = driver.Create(new_raster_path, raster_dataset.RasterXSize, raster_dataset.RasterYSize, 1, gdal.GDT_Float32)

        # Set the geotransform and projection from the original raster
        new_raster_dataset.SetGeoTransform(raster_dataset.GetGeoTransform())
        new_raster_dataset.SetProjection(raster_dataset.GetProjection())

        # Write the new raster data to the new raster
        new_raster_band = new_raster_dataset.GetRasterBand(1)
        new_raster_band.WriteArray(crop_array)

        # Clean up
        new_raster_band.FlushCache()
        new_raster_dataset.FlushCache()
        del raster_dataset, new_raster_dataset, ha_per_cell_ds

        print(f"New raster created at: {new_raster_path}")
        
        # Compute zonal statistics for the current raster
        stats = zonal_stats(kenya_aez_path, new_raster_path, stats="sum", nodata=-9999)

        # Convert the zonal stats result to a DataFrame, adding the year to the DataFrame
        df_stats = pd.DataFrame(stats)
        df_stats['year'] = year

        # Define the mapping based on index
        id_list = ['AN', 'AS', 'CO', 'HR', 'MN', 'MO', 'MS', 'NA']
        # Create the new ID column based on row index
        df_stats['MAPREGH'] = [id_list[i] for i in range(len(df_stats))]
        # Rearranging the columns to put 'ID' at the front
        df_stats = df_stats[['MAPREGH'] + [col for col in df_stats.columns if col != 'MAPREGH']]

        # Append the DataFrame to the list of all statistics
        all_stats.append(df_stats)

# Combine all the zonal stats DataFrames into a single DataFrame
combined_stats_df = pd.concat(all_stats, ignore_index=True)

# Move the 'year' column to the front (left side)
cols = ['year'] + [col for col in combined_stats_df.columns if col != 'year']
combined_stats_df = combined_stats_df[cols]

# Sort the DataFrame by the 'year' column
combined_stats_df = combined_stats_df.sort_values(by=['year', 'MAPREGH']).reset_index(drop=True)
combined_stats_df = combined_stats_df.rename(columns={'sum': 'current_cropland'})

# Save the combined panel DataFrame to a CSV file
output_csv_path = os.path.join(cur_dir, 'current_cropland_kenya_1992_2020.csv')
combined_stats_df.to_csv(output_csv_path, index=False)

print(f"Zonal statistics panel saved to: {output_csv_path}")
