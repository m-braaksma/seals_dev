import pygeoprocessing as pygeo
import os
import pandas as pd
from osgeo import gdal

# Define user and project directories
user_dir = os.path.expanduser('~')
cur_dir = os.path.join(user_dir, os.sep.join(['Files', 'seals', 'projects', 'run_ken_max_land']))
available_land_inputs_path = os.path.join(user_dir, os.sep.join(['Files', 'base_data', 'gtap_invest', 'available_land_inputs']))
available_land_inputs_path_target = os.path.join(user_dir, os.sep.join(['Files', 'base_data', 'gtap_invest', 'available_land_inputs_kenya']))

# Initialize a dictionary to store file paths for raster inputs
raster_inputs = {
    'nutrient_retention_index': os.path.join(available_land_inputs_path, 'nutrient_retention_index_10s.tif'),
    'oxygen_availability_index': os.path.join(available_land_inputs_path, 'oxygen_availability_index_10s.tif'),
    'rooting_conditions_index': os.path.join(available_land_inputs_path, 'rooting_conditions_index_10s.tif'),
    'toxicity_index': os.path.join(available_land_inputs_path, 'toxicity_index_10s.tif'),
    'workability_index': os.path.join(available_land_inputs_path, 'workability_index_10s.tif'),
    'excess_salts_index': os.path.join(available_land_inputs_path, 'excess_salts_index_10s.tif'),
    'nutrient_availability_index': os.path.join(available_land_inputs_path, 'nutrient_availability_index_10s.tif'),
    'TRI': os.path.join(available_land_inputs_path, 'TRI_10s.tif'),
    'minutes_to_market': os.path.join(available_land_inputs_path, 'minutes_to_market_10s.tif'),
    'caloric_yield': os.path.join(available_land_inputs_path, 'caloric_yield_10s.tif'),
    'crop_suitability': os.path.join(available_land_inputs_path, 'crop_suitability_10s.tif'),
    'base_year_lulc_esa': os.path.join(user_dir, os.sep.join(['Files', 'base_data', 'lulc', 'esa', 'lulc_esa_2017.tif'])),
    'ha_per_cell': os.path.join(user_dir, os.sep.join(['Files', 'base_data', 'pyramids', 'ha_per_cell_10sec.tif']))
}

# Define the path to the Kenya vector shapefile
kenya_vector_path = '/Users/mbraaksma/Library/CloudStorage/GoogleDrive-braak014@umn.edu/Shared drives/NatCapTEEMs/Projects/JRC Project/Data/borders/un-ocha/ken_adm_iebc_20191031_shp/ken_admbnda_adm0_iebc_20191031.shp'

# Loop through each raster input and process them
for item in raster_inputs:
    # Perform warping on each raster file to crop it to the boundaries defined by the Kenya vector
    gdal.Warp(os.path.join(available_land_inputs_path_target, item + '.tif'),
              raster_inputs[item],
              cutlineDSName=kenya_vector_path,
              cropToCutline=True,
              srcNodata=pygeo.geoprocessing.get_raster_info(raster_inputs[item])['nodata'][0],
              dstNodata=pygeo.geoprocessing.get_raster_info(raster_inputs[item])['nodata'][0])
    
    # Uncomment the following lines to examine raster properties (min, max values, nodata, and datatype)
    # ds = gdal.Open(raster_inputs[item])
    # array = ds.GetRasterBand(1).ReadAsArray()
    # nodata = pygeo.geoprocessing.get_raster_info(raster_inputs[item])['nodata'][0]
    # datatype = pygeo.geoprocessing.get_raster_info(raster_inputs[item])['datatype']
    # print(item, np.min(array), np.max(array), nodata, datatype)

    # Uncomment the following lines to examine output raster properties after warping
    # ds = gdal.Open(os.path.join(available_land_inputs_path_target, item + '.tif'))
    # array = ds.GetRasterBand(1).ReadAsArray()
    # nodata = pygeo.geoprocessing.get_raster_info(raster_inputs[item])['nodata'][0]
    # datatype = pygeo.geoprocessing.get_raster_info(raster_inputs[item])['datatype']
    # print(item, np.min(array), np.max(array), nodata, datatype)
    # ds = None

# Reclassify the ESA land use data to SEALS classification
base_raster_path = os.path.join(available_land_inputs_path_target, 'base_year_lulc_esa.tif')
target_raster_path = os.path.join(available_land_inputs_path_target, 'base_year_lulc_seals.tif')

# Load the correspondence CSV for reclassification mapping
df = pd.read_csv('/Users/mbraaksma/Files/base_data/seals/default_inputs/esa_seals7_correspondence.csv')
# Create a mapping dictionary from the CSV for reclassification
value_map = pd.Series(df.dst_id.values, index=df.src_id).to_dict()

# Reclassify the base land use raster using the mapping dictionary
pygeo.geoprocessing.reclassify_raster((base_raster_path, 1),
                                        value_map,
                                        target_raster_path,
                                        target_datatype=6,
                                        target_nodata=-9999.0)

# Close the dataset to free up resources
ds = None
