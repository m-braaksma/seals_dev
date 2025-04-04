import os
import hazelbean as hb
import numpy as np
import pandas as pd
import multiprocessing
from matplotlib import pyplot as plt
import geopandas as gpd
from hazelbean import netcdf
from seals_utils import download_google_cloud_blob
from hazelbean import netcdf
from hazelbean.netcdf import describe_netcdf, extract_global_netcdf
import config
from hazelbean import utils
from hazelbean import pyramids
from osgeo import gdal
import netCDF4 as nc
from scipy.interpolate import interp1d

import seals_utils

def regional_change(p):
    if p.run_this:
        
        

        for index, row in p.scenarios_df.iterrows():
            seals_utils.assign_df_row_to_object_attributes(p, row)

            if p.scenario_type != 'baseline':
                if p.regional_projections_input_path:
                    regional_change_vector_path = p.aoi_path
                    coarse_ha_per_cell_path = p.aoi_ha_per_cell_coarse_path
                    # output_path = os.path.join(p.cur_dir, 'regional_change_vector.tif')
                    
                    columns_to_process = p.changing_class_labels
                    regional_change_classes_path = p.regional_projections_input_path
                    
                    for c, year in enumerate(p.years):   
                        if c > 0:                                  
                            previous_year = p.years[c - 1]                                
                        else:
                            previous_year = p.key_base_year
                        
                        region_ids_raster_path = os.path.join(p.cur_dir, 'region_ids.tif')
                        output_dir = os.path.join(p.cur_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year)) 
                        scenario_label = p.scenario_label    
                        output_filename_end = '_' + str(year) + '_' + str(previous_year) + '_ha_diff_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '.tif'
                        
                        # TODOO I should have iterated on class label here, then called the util file only on that one
                        seals_utils.convert_regional_change_to_coarse(regional_change_vector_path, regional_change_classes_path, coarse_ha_per_cell_path, scenario_label, output_dir, output_filename_end, columns_to_process, region_ids_raster_path=region_ids_raster_path, distribution_algorithm='proportional', coarse_change_raster_path=None)
                                    
                
                        
                        if hb.path_exists(p.coarse_projections_input_path): # Then it is a regional shift with a coarse spatial projection
                            for column in columns_to_process:
                                
                                local_dir = os.path.join(p.coarse_simplified_ha_difference_from_previous_year_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year)) 
                                local_filename = column + '_' + str(year) + '_' + str(previous_year) + '_ha_diff_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '.tif'
                                local_path = os.path.join(local_dir, local_filename)
                                hb.log("loading the coarse gridded projection raster from " + local_path)
                                coarse_projections = hb.as_array(local_path)
                                
                                output_filename = column + '_' + str(year) + '_' + str(previous_year) + '_ha_diff_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '.tif'
                                output_path = os.path.join(output_dir, output_filename)
                                
                                # Now add the coarse_projections to the regional_coarsified_raster
                                regional_coarsified_raster = hb.as_array(output_path)
                                regional_coarsified_raster += coarse_projections
                                hb.save_array_as_geotiff(regional_coarsified_raster, output_path, local_path)
                        else:
                            "no need to shift it then."
                else:
                    hb.log('No regional change listed, so not shifting regions.')    


def coarse_change(p):
    # Make folder for all generated data.
    # Just to create folder
    pass

def download_base_data(p):
    task_note = """" 
Download the base data. Unlike other tasks, this task puts the files into a tightly defined directory structure rooted at  p.base_data_dir    
    """
    if p.run_this:

        # Generated based on if required files actually exist.
        p.required_base_data_urls = []
        p.required_base_data_dst_paths = []

        print('replaced by p.get_path')
        # flattened_list = hb.flatten_nested_dictionary(p.required_base_data_paths, return_type='values')

        # hb.debug('Script requires the following Base Data to be in your base_data_dir\n' + hb.pp(p.required_base_data_paths, return_as_string=True))
        # for path in p.required_base_data_paths:
        #     if not hb.path_exists(path) and not path == 'use_generated' and not path == 'calibration_task':
        #         hb.log('Path did not exist, so adding it to urls to download: ' + str(path))

        #         # HACK, should have made this cleaner

        #         if p.base_data_dir in path:

        #             url_from_path =  path.split(os.path.split(p.base_data_dir)[1])[1].replace('\\', '/')
        #             url_from_path = 'base_data' + url_from_path

        #             p.required_base_data_urls.append(url_from_path)
        #             p.required_base_data_dst_paths.append(path)

        # if len(p.required_base_data_urls) > 0:
        #     for c, blob_url in enumerate(p.required_base_data_urls):

        #         # The data_credentials_path file needs to be given to the user with a specific service account email attached. Generated via the gcloud CMD line, described in new_computer.
        #         filename = os.path.split(blob_url)[1]
        #         dst_path = p.required_base_data_dst_paths[c]
        #         if not hb.path_exists(dst_path): # Check one last time to ensure that it wasn't added twice.
        #             download_google_cloud_blob(p.input_bucket_name, blob_url, p.data_credentials_path, dst_path)



def lulc_as_coarse_states(p):
    """This task is not needed for the BASE seals workflow but is for either calibration runs, or runs that are based on adjusting the inputs to match existing LULC."""
    p.L.warning('This task doesnt speed up enough when using testing. consider adding it to base data.')
    """For the purposes of calibration, create change-matrices for each coarse grid-cell based on two observed ESA lulc maps.
    Does something similar to prepare_lulc"""

    from hazelbean.calculation_core.cython_functions import calc_change_matrix_of_two_int_arrays
   
    if p.run_this:
        p.ha_per_cell_coarse = hb.ArrayFrame(p.global_ha_per_cell_course_path)
        p.coarse_match = hb.ArrayFrame(p.global_ha_per_cell_course_path)


        # TODO This needs to be fixed so that it calculates on the reclassification in use (currently it's using simplified hardcoded but we need it to shift to)
        output_arrays = np.zeros((len(p.class_indices), p.coarse_match.shape[0], p.coarse_match.shape[1]))
        calc_change_matrix = False
        numpy_output_path = os.path.join(p.cur_dir, 'change_matrices.npy')
        if not hb.path_exists(numpy_output_path) and calc_change_matrix:
            t1 = hb.ArrayFrame(p.training_start_year_simplified_lulc_path)
            t2 = hb.ArrayFrame(p.training_end_year_simplified_lulc_path)
            fine_cells_per_coarse_cell = round((p.ha_per_cell_coarse.cell_size / t1.cell_size) ** 2)
            aspect_ratio = t1.num_cols / p.coarse_match.num_cols
            for r in range(p.coarse_match.num_rows):
                hb.log('Processing observed change row', r)
                for c in range(p.coarse_match.num_cols):
                    t1_subarray = t1.data[int(r * aspect_ratio): int((r + 1) * aspect_ratio), int(c * aspect_ratio): int((c + 1) * aspect_ratio)]
                    t2_subarray = t2.data[int(r * aspect_ratio): int((r + 1) * aspect_ratio), int(c * aspect_ratio): int((c + 1) * aspect_ratio)]
                    # ha_per_cell_subarray = p.ha_per_cell_coarse.data[int(r * aspect_ratio): int((r + 1) * aspect_ratio), int(c * aspect_ratio): int((c + 1) * aspect_ratio)]

                    ha_per_cell_coarse_this_subarray = p.ha_per_cell_coarse.data[r, c]
                    change_matrix, counters = calc_change_matrix_of_two_int_arrays(t1_subarray.astype(np.int), t2_subarray.astype(np.int), p.class_indices)
                    # Potentially unused relic from prepare_lulc
                    full_change_matrix = np.zeros((len(p.class_indices), len(p.class_indices)))
                    vector = seals_utils.calc_change_vector_of_change_matrix(change_matrix)

                    ha_per_cell_this_subarray = p.ha_per_cell_coarse.data[r, c] / fine_cells_per_coarse_cell

                    if vector:
                        for i in p.class_indices:
                            output_arrays[i - 1, r, c] = vector[i - 1] * ha_per_cell_this_subarray
                    else:
                        output_arrays[i, r, c] = 0.0

            for c, class_label in enumerate(p.class_labels):
                output_path = os.path.join(p.cur_dir, class_label + '_observed_change.tif')
                hb.save_array_as_geotiff(output_arrays[c], output_path, p.coarse_match.path)
            hb.save_array_as_npy(output_arrays, numpy_output_path)

            # Stores all of the classes in a 3d array ready for validation exercises below.
            change_3d = hb.load_npy_as_array(numpy_output_path)

        # Sometimes you don't want the change but need the actual state maps (ala luh) implied by a given ESA map.
        # Here calculates a cython function that downscales a fine_categorical to a stack of coarse_continuous 3d
        # Test that this is equivilent to change_3d


        p.base_year_simplified_lulc_path = p.global_esa_simplified_lulc_paths_by_year[p.baseline_years[0]]

        p.observed_lulc_paths_to_calculate_states = [ p.base_year_simplified_lulc_path]
        # p.observed_lulc_paths_to_calculate_states = [p.global_esa_simplified_lulc_paths_by_year[2000], p.global_esa_simplified_lulc_paths_by_year[2015], p.base_year_simplified_lulc_path]

        p.years_to_calculate_states = p.baseline_years
        # p.years_to_calculate_states = [2000, 2015] + p.baseline_years
        p.observed_state_paths = {}
        for year in p.years_to_calculate_states:
            p.observed_state_paths[year] = {}
            for class_label in p.class_labels + p.nonchanging_class_labels:
                p.observed_state_paths[year][class_label] = os.path.join(p.cur_dir, hb.file_root(p.global_esa_simplified_lulc_paths_by_year[year]) + '_state_' + str(class_label) + '_observed.tif')

        global_bb = hb.get_bounding_box(p.observed_lulc_paths_to_calculate_states[0])
        # TODOO Here incorporate test-mode bb.
        # stitched_bb = hb.get_bounding_box()

        for c, year in enumerate(p.years_to_calculate_states):
            if not hb.path_exists(p.observed_state_paths[year][p.class_labels[0]], verbose=0):
            # if not all([hb.path_exists(i) for i in p.observed_state_paths[year]]):

                fine_path = p.observed_lulc_paths_to_calculate_states[c]
                hb.log('Calculating coarse_state_stack from ' + fine_path)

                output_dir = p.cur_dir

                fine_input_array = hb.load_geotiff_chunk_by_bb(fine_path, global_bb, datatype=5)
                coarse_match_array = hb.load_geotiff_chunk_by_bb(p.coarse_match_path, global_bb, datatype=6)

                chunk_edge_length = int(fine_input_array.shape[0] / coarse_match_array.shape[0])

                max_value_to_summarize = 8
                values_to_summarize = np.asarray(p.class_indices + p.nonchanging_class_indices, dtype=np.int32)

                import hazelbean.calculation_core
                coarse_state_3d = hb.calculation_core.cython_functions.calculate_coarse_state_stack_from_fine_classified(fine_input_array,
                                                                      coarse_match_array,
                                                                      values_to_summarize,
                                                                      max_value_to_summarize)

                c = 0
                for k, v in p.observed_state_paths[year].items():
                    # Convert a count of states to a proportion of grid-cell
                    a = coarse_state_3d[c].astype(np.float32) / np.float32(chunk_edge_length ** 2)
                    hb.save_array_as_geotiff(a, v, p.coarse_match_path, data_type=6)
                    c += 1

        # Clip back to AOI


        c = 0
        for k, v in p.observed_state_paths[year].items():
            hb.load_geotiff_chunk_by_bb(v, p.bb, output_path=hb.suri(v, 'aoi'))


def check_netcdf_file(nc_path, var_name):
    """Check if the NetCDF file exists and contains the specified variable."""
    if not os.path.exists(nc_path):
        print(f"File does not exist: {nc_path}")
        return False
    try:
        with nc.Dataset(nc_path, 'r') as ds:
            if var_name in ds.variables:
                print(f"Variable '{var_name}' is present in the file.")
                return True
            else:
                print(f"Variable '{var_name}' is not found. Available variables: {list(ds.variables.keys())}")
                return False
    except Exception as e:
        print(f"Failed to read NetCDF file: {e}")
        return False

def interpolate_years(lower_data, upper_data, target_year, lower_year, upper_year):
    """Interpolates data between two bands linearly based on the target year."""
    factor = (target_year - lower_year) / (upper_year - lower_year)
    interpolated_data = lower_data + (upper_data - lower_data) * factor
    return interpolated_data

def load_correspondence_dict(correspondence_path):
    """Load the correspondence dictionary from a CSV file."""
    df = pd.read_csv(correspondence_path)
    correspondence_dict = {row['src_id']: row['src_label'] for index, row in df.iterrows()}
    return correspondence_dict

def extract_btc_netcdf(src_nc_path, output_dir, filter_dict, correspondence_dict):
    """Extract specific data from a NetCDF file based on a filtering dictionary and save as GeoTIFFs."""
    var_name = 'LC_area_share'
    if not check_netcdf_file(src_nc_path, var_name):
        return
    
    full_src_nc_path = f'NETCDF:"{src_nc_path}":{var_name}'
    ds = gdal.Open(full_src_nc_path)
    if ds is None:
        print(f"Failed to open source NetCDF file: {src_nc_path}")
        return

    available_years = [2010 + i * 10 for i in range((2100 - 2010) // 10 + 1)]
    acceptable_years = [int(year) for year in filter_dict['time']]
    print(f"Years to filter by: {acceptable_years}")

    for target_year in acceptable_years:
        time_dir = os.path.join(output_dir, f"time_{target_year}")
        os.makedirs(time_dir, exist_ok=True)

        for lc_class in range(1, ds.RasterCount // len(available_years) + 1):
            if target_year in available_years:
                band_index = (available_years.index(target_year) * (ds.RasterCount // len(available_years))) + lc_class
                band = ds.GetRasterBand(band_index)
                data_array = band.ReadAsArray()
                data_type = band.DataType
                ndv = band.GetNoDataValue()
            else:
                lower_year = max([year for year in available_years if year < target_year], default=2010)
                upper_year = min([year for year in available_years if year > target_year], default=2100)
                lower_band_index = (available_years.index(lower_year) * (ds.RasterCount // len(available_years))) + lc_class
                upper_band_index = (available_years.index(upper_year) * (ds.RasterCount // len(available_years))) + lc_class
                lower_band = ds.GetRasterBand(lower_band_index)
                upper_band = ds.GetRasterBand(upper_band_index)
                lower_data = lower_band.ReadAsArray()
                upper_data = upper_band.ReadAsArray()
                data_array = interpolate_years(lower_data, upper_data, target_year, lower_year, upper_year)
                data_type = lower_band.DataType  # Assuming both bands have the same data type
                ndv = lower_band.GetNoDataValue()  # Assuming both bands have the same NoDataValue
                print(f"Interpolated data for year {target_year}, class {lc_class}.")

            src_label = correspondence_dict.get(lc_class, f"lc_class_{lc_class}")
            band_name = f"{src_label}.tif"
            dst_file_path = os.path.join(time_dir, band_name)

            driver = gdal.GetDriverByName('GTiff')
            out_ds = driver.Create(dst_file_path, ds.RasterXSize, ds.RasterYSize, 1, data_type)
            if out_ds is None:
                print(f"Failed to create output file: {dst_file_path}")
                continue

            out_ds.GetRasterBand(1).WriteArray(data_array)
            out_ds.GetRasterBand(1).SetNoDataValue(ndv)
            out_ds.SetGeoTransform(ds.GetGeoTransform())
            out_ds.SetProjection('EPSG:4326')  # Set the projection to WGS84

            out_ds = None
            print(f"Saved: {dst_file_path}")

def coarse_extraction_btc(p):
    """Create an empty folder dir to hold all coarse intermediate outputs, such as per-year changes in LU hectarage."""
    if p.run_this:
        p.coarse_correspondence_dict = load_correspondence_dict(p.coarse_correspondence_path)
        for index, row in list(p.scenarios_df.iterrows()):
            seals_utils.assign_df_row_to_object_attributes(p, row)
            hb.log('Extracting coarse states for scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)) + ' with row ' + str([i for i in row]))
            hb.debug('Analyzing row:\n' + str(row))

            if p.scenario_type == 'baseline':
                if hb.path_exists(os.path.join(p.input_dir, p.coarse_projections_input_path)):
                    src_nc_path = os.path.join(p.input_dir, p.coarse_projections_input_path)
                elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_projections_input_path)):
                    src_nc_path = os.path.join(p.base_data_dir, p.coarse_projections_input_path)
                else:
                    hb.log('Could not find ' + str(p.coarse_projections_input_path) + ' in either ' + str(p.input_dir) + ' or ' + str(p.base_data_dir))

                dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.model_label)
                
                filter_dict = {'time': p.years}
                # print(p.years)
                # print(p.coarse_correspondence_dict)

                if not hb.path_exists(dst_dir):
                    extract_btc_netcdf(src_nc_path, dst_dir, filter_dict, p.coarse_correspondence_dict)
                for year in p.years:
                    out_dst_dir = os.path.join(dst_dir, 'time_' + str(year))
                    hb.create_directories(out_dst_dir)
                
                    for lc_class_name in p.coarse_correspondence_dict.values():
                        out_dst_lc_path = os.path.join(out_dst_dir, f"{lc_class_name}.tif")
                        hb.make_path_global_pyramid(out_dst_lc_path)
            else:
                if p.coarse_projections_input_path:
                    if hb.path_exists(os.path.join(p.input_dir, p.coarse_projections_input_path)):
                        src_nc_path = os.path.join(p.input_dir, p.coarse_projections_input_path)
                    elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_projections_input_path)):
                        src_nc_path = os.path.join(p.base_data_dir, p.coarse_projections_input_path)
                    else:
                        hb.log('No understandable input source.')
                else:
                    hb.log('No coarse change listed')

                dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label)
                filter_dict = {'time': p.years}
                
                # print(p.years)
                # print(p.coarse_correspondence_dict)

                if not hb.path_exists(dst_dir):
                    extract_btc_netcdf(src_nc_path, dst_dir, filter_dict, p.coarse_correspondence_dict)
                for year in p.years:
                    out_dst_dir = os.path.join(dst_dir, 'time_' + str(year))
                    hb.create_directories(out_dst_dir)
                
                    for lc_class_name in p.coarse_correspondence_dict.values():
                        out_dst_lc_path = os.path.join(out_dst_dir, f"{lc_class_name}.tif")
                        hb.make_path_global_pyramid(out_dst_lc_path)
                    
        p.coarse_extraction_dir = p.coarse_extraction_btc_dir





def coarse_extraction(p):
    # Extract coarse change from source
    doc = """Create a empty folder dir. This will hold all of the coarse intermediate outputs, such as per-year changes in lu hectarage. Naming convention matches source. After reclassification this will be in destination conventions.  """
    if p.run_this:
         
        
        # if p.report_netcdf_read_analysis:
            
        for index, row in list(p.scenarios_df.iterrows()):
            seals_utils.assign_df_row_to_object_attributes(p, row)

            hb.log('Extracting coarse states for scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)) + ' with row ' + str([i for i in row]))
            hb.debug('Analyzing row:\n' + str(row))

            if p.scenario_type == 'baseline':

                if hb.path_exists(os.path.join(p.input_dir, p.coarse_projections_input_path)):
                    src_nc_path = os.path.join(p.input_dir, p.coarse_projections_input_path)
                elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_projections_input_path)):
                    src_nc_path = os.path.join(p.base_data_dir, p.coarse_projections_input_path)
                else:
                    hb.log('Could not find ' + str(p.coarse_projections_input_path) + ' in either ' + str(p.input_dir) + ' or ' + str(p.base_data_dir))

                dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.model_label)

                adjustment_dict = {
                    'time': row['time_dim_adjustment'],  # eg +850 or *5+14 eg
                }

                filter_dict = {
                    'time': p.years,
                }

                if not hb.path_exists(dst_dir):
                    extract_global_netcdf(src_nc_path, dst_dir, adjustment_dict, filter_dict, skip_if_exists=True, verbose=0)
            else:
                if p.coarse_projections_input_path:
                    if hb.path_exists(os.path.join(p.input_dir, p.coarse_projections_input_path)):
                        src_nc_path = os.path.join(p.input_dir, p.coarse_projections_input_path)
                    elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_projections_input_path)):
                        src_nc_path = os.path.join(p.base_data_dir, p.coarse_projections_input_path)
                    else:
                        hb.log('No understandible input_source.')
                else:
                    hb.log('No coarse change listed')
               
                dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label)



                adjustment_dict = {
                    'time': row['time_dim_adjustment'],  # or *5+14 eg
                }

                filter_dict = {
                    'time': p.years,
                }
                if not hb.path_exists(dst_dir):
                    extract_global_netcdf(src_nc_path, dst_dir, adjustment_dict, filter_dict, skip_if_exists=True, verbose=0)

                
def coarse_simplified_proportion(p):
    # Reclassify coarse source to simplified scheme 
    task_note = """This function converts the extracted geotiffs from the source 
classification to the the destination classification, potentially aggregating classes as it goes. """\
    
    if p.run_this:
        
        for index, row in p.scenarios_df.iterrows():
            seals_utils.assign_df_row_to_object_attributes(p, row)
            hb.log('Converting coarse_extraction to simplified proportion for scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)) + ' with row ' + str([i for i in row]))


            
            # p.coarse_correspondence_path = hb.get_first_extant_path(p.coarse_correspondence_path, [p.input_dir, p.base_data_dir])
            p.coarse_correspondence_dict = hb.utils.get_reclassification_dict_from_df(p.coarse_correspondence_path, 'src_id', 'dst_id', 'src_label', 'dst_label')
            
            # if hb.path_exists(os.path.join(p.input_dir, p.coarse_correspondence_path)):
            #     p.coarse_correspondence_dict = hb.utils.get_reclassification_dict_from_df(os.path.join(p.input_dir, p.coarse_correspondence_path), 'src_id', 'dst_id', 'src_label', 'dst_label')
            # elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_correspondence_path)):
            #     p.coarse_correspondence_dict = hb.utils.get_reclassification_dict_from_df(os.path.join(p.base_data_dir, p.coarse_correspondence_path), 'src_id', 'dst_id', 'src_label', 'dst_label')
            # else:
            #     raise NameError('Unable to find ' + p.coarse_correspondence_path)

            if p.scenario_type == 'baseline':

                for year in p.years:
                    
                    dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.model_label, str(year))
                    hb.create_directories(dst_dir)


                    for k, v in p.coarse_correspondence_dict['dst_to_src_reclassification_dict'].items():
                        # pos = list(p.coarse_correspondence_dict['dst_to_src_reclassification_dict'].keys()).index(k)
                        output_array = None


                        # dst_class_label = list(p.coarse_correspondence_dict['dst_labels'])[pos]
                        
                        dst_class_label = p.coarse_correspondence_dict['dst_ids_to_labels'][k]
                        dst_path = os.path.join(dst_dir, str(dst_class_label) + '_prop_' + p.exogenous_label + '_' + p.model_label + '_' + str(year) + '.tif')

                        if not hb.path_exists(dst_path):
                            
                            # Notice that here implies the relationship from src to simplified is many to one.
                            for c, i in enumerate(v):

                                if p.lc_class_varname == 'all_variables':
                                    src_class_label = p.coarse_correspondence_dict['src_ids_to_labels'][i]
                                    # src_class_label = p.coarse_correspondence_dict['src_labels'][c]
                                    src_dir = os.path.join(p.coarse_extraction_dir, p.exogenous_label, p.model_label, 'time_' + str(year))

                                    src_path = os.path.join(src_dir, src_class_label + '.tif')
                                    ndv = hb.get_ndv_from_path(src_path)
                                    coarse_shape = hb.get_shape_from_dataset_path(src_path)
                                    if output_array is None:
                                        output_array = np.zeros(coarse_shape, dtype=np.float64)
                                    input_array = hb.as_array(src_path)
                                    output_array += np.where(input_array != ndv, input_array, 0)
                                else:
                                    # Then the lc_class vars have been embeded as dimensions... grrrr
                                    src_class_label = p.coarse_correspondence_dict['src_ids_to_labels'][i]
                                    # src_class_label = p.coarse_correspondence_dict['src_labels'][c]
                                    src_dir = os.path.join(p.coarse_extraction_dir, p.exogenous_label, p.model_label, 'time_' + str(year), p.dimensions[-1] + '_' + str(i))

                                    src_path = os.path.join(src_dir, p.lc_class_varname + '.tif')
                                    ndv = hb.get_ndv_from_path(src_path)
                                    coarse_shape = hb.get_shape_from_dataset_path(src_path)
                                    if output_array is None:
                                        output_array = np.zeros(coarse_shape, dtype=np.float64)
                                    input_array = hb.as_array(src_path)
                                    output_array += np.where(input_array != ndv, input_array, 0)

                            hb.save_array_as_geotiff(output_array, dst_path, src_path)
                            output_array = None
            else:
                for year in p.years:
                    
                    dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year))
                    hb.create_directories(dst_dir)


                    for k, v in p.coarse_correspondence_dict['dst_to_src_reclassification_dict'].items():
                        # pos = list(p.coarse_correspondence_dict['dst_to_src_reclassification_dict'].keys()).index(k)
                        output_array = None
                        dst_class_label = p.coarse_correspondence_dict['dst_ids_to_labels'][k]
                        # dst_class_label = list(p.coarse_correspondence_dict['dst_labels'])[pos]
                        dst_path = os.path.join(dst_dir, str(dst_class_label) + '_prop_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '_' + str(year) + '.tif')

                        if not hb.path_exists(dst_path):
                            
                            # Notice that here implies the relationship from src to simplified is many to one.
                            for c, i in enumerate(v):

                                if p.lc_class_varname == 'all_variables':
                                    src_class_label = p.coarse_correspondence_dict['src_ids_to_labels'][i]
                                    # src_class_label = p.coarse_correspondence_dict['src_labels'][c]
                                    src_dir = os.path.join(p.coarse_extraction_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, 'time_' + str(year))

                                    src_path = os.path.join(src_dir, src_class_label + '.tif')
                                    ndv = hb.get_ndv_from_path(src_path)
                                    coarse_shape = hb.get_shape_from_dataset_path(src_path)
                                    if output_array is None:
                                        output_array = np.zeros(coarse_shape, dtype=np.float64)
                                    input_array = hb.as_array(src_path)
                                    output_array += np.where(input_array != ndv, input_array, 0)
                                else:
                                    # Then the lc_class vars have been embeded as dimensions... grrrr
                                    src_class_label = p.coarse_correspondence_dict['src_ids_to_labels'][i]
                                    # src_class_label = p.coarse_correspondence_dict['src_labels'][c]
                                    src_dir = os.path.join(p.coarse_extraction_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, 'time_' + str(year), p.dimensions[-1] + '_' + str(i))

                                    src_path = os.path.join(src_dir, p.lc_class_varname + '.tif')
                                    ndv = hb.get_ndv_from_path(src_path)

                                    coarse_shape = hb.get_shape_from_dataset_path(src_path)
                                    if output_array is None:
                                        output_array = np.zeros(coarse_shape, dtype=np.float64)
                                    input_array = hb.as_array(src_path)
                                    output_array += np.where(input_array != ndv, input_array, 0)                        
                            hb.save_array_as_geotiff(output_array, dst_path, src_path)
                            output_array = None

                
def coarse_simplified_ha(p):

    # Converts proportion to ha for each input.
    # NOTE this duplicates implicit work in the tasks with ha_differencel, but those didn't save the intermedaite outputs so i made this.
    
    if p.run_this:
        
        for index, row in p.scenarios_df.iterrows():
            seals_utils.assign_df_row_to_object_attributes(p, row)
            hb.log('Converting simplified proportion to simplified_ha for scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)) + ' with row ' + str([i for i in row]))

            if p.scenario_type == 'baseline':
                for year in p.base_years:
                    src_dir = os.path.join(p.coarse_simplified_proportion_dir, p.exogenous_label, p.model_label, str(year))
                    dst_dir  = os.path.join(p.cur_dir, p.exogenous_label, p.model_label, str(year))
                    
                    for class_c, class_label in enumerate(p.changing_class_labels):
                        dst_path = os.path.join(dst_dir, str(class_label) + '_ha_' + p.exogenous_label + '_' + p.model_label + '_' + str(year) + '.tif')

                        if not hb.path_exists(dst_path):
                            
                            src_path = os.path.join(src_dir, class_label + '_prop_' + p.exogenous_label + '_' + p.model_label + '_' + str(year) + '.tif')
                            
                            ha_array = hb.as_array(p.aoi_ha_per_cell_coarse_path)
                            # input_prop_array = hb.as_array(src_path)
                            input_prop_array = hb.load_geotiff_chunk_by_bb(src_path, p.bb)
                            output_ha = ha_array * input_prop_array
                            
                            hb.save_array_as_geotiff(output_ha, dst_path, p.aoi_ha_per_cell_coarse_path)
                            

            else:
                for year in p.years:
                    
                    src_dir = os.path.join(p.coarse_simplified_proportion_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year))
                    dst_dir  = os.path.join(p.cur_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year))
                    
                    for class_c, class_label in enumerate(p.changing_class_labels):

                        dst_path = os.path.join(dst_dir, str(class_label) + '_ha_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '_' + str(year) + '.tif')

                        if not hb.path_exists(dst_path):
                            
                            src_path = os.path.join(src_dir, str(class_label) + '_prop_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '_' + str(year) + '.tif')
                            
                            ha_array = hb.as_array(p.aoi_ha_per_cell_coarse_path)
                            input_prop_array = hb.load_geotiff_chunk_by_bb(src_path, p.bb)
                            output_ha = ha_array * input_prop_array
                            
                            hb.save_array_as_geotiff(output_ha, dst_path, p.aoi_ha_per_cell_coarse_path)                
                
                
                

def coarse_simplified_ha_difference_from_base_year(p):

    if p.run_this:      

        for index, row in p.scenarios_df.iterrows():
            seals_utils.assign_df_row_to_object_attributes(p, row)
            hb.log('Converting coarse_extraction to simplified proportion for scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)) + ' with row ' + str([i for i in row]))
            
            if hb.path_exists(os.path.join(p.input_dir, p.coarse_correspondence_path)):
                p.lulc_correspondence_dict = hb.utils.get_reclassification_dict_from_df(os.path.join(p.input_dir, p.coarse_correspondence_path), 'src_id', 'dst_id', 'src_label', 'dst_label')
            elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_correspondence_path)):
                p.lulc_correspondence_dict = hb.utils.get_reclassification_dict_from_df(os.path.join(p.base_data_dir, p.coarse_correspondence_path), 'src_id', 'dst_id', 'src_label', 'dst_label')
            else:
                raise NameError('Unable to find ' + p.coarse_correspondence_path)
                        
            cell_size = hb.get_cell_size_from_path(os.path.join(p.base_data_dir, p.coarse_projections_input_path))
            
            # correct_global_ha_per_cell_path = p.ha_per_cell_paths[cell_size * 3600.0] 
            # correct_aoi_ha_per_cell_path = os.path.join(p.intermediate_dir, 'project_aoi', 'pyramids', 'aoi_ha_per_cell_' + str(int(cell_size * 3600.0)) + '.tif')
            # hb.clip_raster_by_bb(correct_global_ha_per_cell_path, p.bb, correct_aoi_ha_per_cell_path)
            ha_per_cell_array = hb.as_array(p.aoi_ha_per_cell_coarse_path)
            

            if p.scenario_type != 'baseline':


                for c, i in enumerate(p.coarse_correspondence_dict['dst_ids']):
                   
                    # pos = list(coarse_correspondence_dict['values'].keys()).index(k)


                    output_array = None
                    dst_class_label = p.coarse_correspondence_dict['dst_labels'][c]


                    # Get the input_path to the baseline_reference_label from the scenarios_df
                    baseline_reference_label = row['baseline_reference_label']
                    baseline_reference_row = p.scenarios_df.loc[p.scenarios_df['scenario_label'] == baseline_reference_label]
                    baseline_exogenous_label = baseline_reference_row['exogenous_label'].values[0]
                    baseline_reference_model = baseline_reference_row['model_label'].values[0]
                    
                    base_year = int(row['key_base_year'])

                    base_year_dir = os.path.join(p.coarse_simplified_proportion_dir, baseline_exogenous_label, baseline_reference_model, str(base_year))
                    base_year_path = os.path.join(base_year_dir, str(dst_class_label) + '_prop_' + baseline_exogenous_label + '_' + baseline_reference_model + '_' + str(base_year) + '.tif')
                    # base_year_path = os.path.join(base_year_dir, p.lulc_simplification_label + '_' + v + '.tif')

                    for year in p.years:
                    
                        src_dir = os.path.join(p.coarse_simplified_proportion_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year))
                        src_path = os.path.join(src_dir, str(dst_class_label) + '_prop_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '_' + str(year) + '.tif')

                        dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year))
                        hb.create_directories(dst_dir)

                        dst_path = os.path.join(dst_dir, dst_class_label + '_' + str(year) + '_' + str(base_year) + '_ha_diff_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '.tif')

                        if not hb.path_exists(dst_path):

                            input_array = hb.load_geotiff_chunk_by_bb(src_path, p.bb)
                            input_ndv = hb.get_ndv_from_path(src_path)

                            base_year_array = hb.load_geotiff_chunk_by_bb(base_year_path, p.bb)
                            base_year_ndv = hb.get_ndv_from_path(base_year_path)

                            input_array = np.where(input_array == input_ndv, 0, input_array)
                            base_year_array = np.where(base_year_array == base_year_ndv, 0, base_year_array)                            

                            if input_array.shape != base_year_array.shape:
                                raise NameError('input_array.shape != base_year_array.shape: ' + str(input_array.shape) + ' != ' + str(base_year_array.shape) + '. This means that the coarse definition of the scenario that you are subtracting from the coarse definition of the baseline is mixing resolutions. You probably want to resample one of the two layers first.') 
                            current_array = (input_array - base_year_array) * ha_per_cell_array

                            hb.save_array_as_geotiff(current_array, dst_path, p.aoi_ha_per_cell_coarse_path)
                    


def coarse_simplified_ha_difference_from_previous_year(p):
    # Calculate LUH2_simplified difference from base year
    task_documentation = """Calculates LUH2_simplified difference from base year"""
    if p.run_this:      

        for index, row in p.scenarios_df.iterrows():
            seals_utils.assign_df_row_to_object_attributes(p, row)
            hb.log('Converting coarse_extraction to simplified proportion for scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)) + ' with row ' + str([i for i in row]))
            
            if hb.path_exists(os.path.join(p.input_dir, p.coarse_correspondence_path)):
                p.lulc_correspondence_dict = hb.utils.get_reclassification_dict_from_df(os.path.join(p.input_dir, p.coarse_correspondence_path), 'src_id', 'dst_id', 'src_label', 'dst_label')
            elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_correspondence_path)):
                p.lulc_correspondence_dict = hb.utils.get_reclassification_dict_from_df(os.path.join(p.base_data_dir, p.coarse_correspondence_path), 'src_id', 'dst_id', 'src_label', 'dst_label')
            else:
                raise NameError('Unable to find ' + p.coarse_correspondence_path)
                        
            cell_size = hb.get_cell_size_from_path(p.ha_per_cell_coarse_path)
            
            # correct_global_ha_per_cell_path = p.ha_per_cell_coarse_path
            correct_aoi_ha_per_cell_path = os.path.join(p.intermediate_dir, 'project_aoi', 'pyramids', 'aoi_ha_per_cell_' + str(int(cell_size * 3600.0)) + 'sec.tif')
            # hb.clip_raster_by_bb(correct_global_ha_per_cell_path, p.bb, correct_aoi_ha_per_cell_path)
            ha_per_cell_array = hb.as_array(p.aoi_ha_per_cell_coarse_path)
            

            

            if p.scenario_type != 'baseline':


                for c, i in enumerate(p.coarse_correspondence_dict['dst_ids']):
                   
                    # pos = list(coarse_correspondence_dict['values'].keys()).index(k)


                    output_array = None
                    dst_class_label = p.coarse_correspondence_dict['dst_labels'][c]


                    # Get the input_path to the baseline_reference_label from the scenarios_df
                    baseline_reference_label = row['baseline_reference_label']
                    baseline_reference_row = p.scenarios_df.loc[p.scenarios_df['scenario_label'] == baseline_reference_label]
                    baseline_exogenous_label = baseline_reference_row['exogenous_label'].values[0]
                    baseline_reference_model = baseline_reference_row['model_label'].values[0]
                    current_starting_year = None
                    previous_year = None

                    # Process the current row and its corresponding baseline to get the full set of years involved
                    
                    
                    # starting_year = int(row['key_base_year'])
                    # base_year = int(row['key_base_year'])

                    # base_year_dir = os.path.join(p.coarse_simplified_proportion_dir, baseline_exogenous_label, baseline_reference_model, str(base_year))
                    # base_year_path = os.path.join(base_year_dir, str(dst_class_label) + '_prop_' + baseline_exogenous_label + '_' + baseline_reference_model + '_' + str(base_year) + '.tif')
                    # # base_year_path = os.path.join(base_year_dir, p.lulc_simplification_label + '_' + v + '.tif')

                    for year in p.years:


                        if current_starting_year is None:
                            base_year = int(row['key_base_year'])
                            current_starting_year = base_year
                            current_starting_year_dir = os.path.join(p.coarse_simplified_proportion_dir, baseline_exogenous_label, baseline_reference_model, str(current_starting_year))
                            current_starting_year_path = os.path.join(current_starting_year_dir, str(dst_class_label) + '_prop_' + baseline_exogenous_label + '_' + baseline_reference_model + '_' + str(current_starting_year) + '.tif')
                        else:

                            
                            current_starting_year = previous_year
                            current_starting_year_dir = os.path.join(p.coarse_simplified_proportion_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(current_starting_year))
                            # current_starting_year_dir = os.path.join(p.coarse_simplified_proportion_dir, baseline_exogenous_label, baseline_reference_model, str(current_starting_year))
                            current_starting_year_path = os.path.join(current_starting_year_dir, str(dst_class_label) + '_prop_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '_' + str(current_starting_year) + '.tif')
                            # current_starting_year_path = os.path.join(current_starting_year_dir, str(dst_class_label) + '_prop_' + baseline_exogenous_label + '_' + baseline_reference_model + '_' + str(current_starting_year) + '.tif')
                            
                        current_ending_year_src_dir = os.path.join(p.coarse_simplified_proportion_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year))
                        current_ending_year_src_path = os.path.join(current_ending_year_src_dir, str(dst_class_label) + '_prop_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '_' + str(year) + '.tif')

                        current_ending_year_dst_dir = os.path.join(p.cur_dir, p.exogenous_label, p.climate_label, p.model_label, p.counterfactual_label, str(year))
                        hb.create_directories(current_ending_year_dst_dir)

                        current_ending_year_dst_path = os.path.join(current_ending_year_dst_dir, dst_class_label + '_' + str(year) + '_' + str(current_starting_year) + '_ha_diff_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '.tif')

                        if not hb.path_exists(current_ending_year_dst_path):

                            ending_year_array = hb.load_geotiff_chunk_by_bb(current_ending_year_src_path, p.bb)
                            ending_year_ndv = hb.get_ndv_from_path(current_ending_year_src_path)

                            starting_year_array = hb.load_geotiff_chunk_by_bb(current_starting_year_path, p.bb)
                            starting_year_ndv = hb.get_ndv_from_path(current_starting_year_path)

                            ending_year_array = np.where(ending_year_array == ending_year_ndv, 0, ending_year_array)
                            starting_year_array = np.where(starting_year_array == starting_year_ndv, 0, starting_year_array)                            

                            if ending_year_array.shape != starting_year_array.shape:
                                raise NameError('ending_year_array.shape != starting_year_array.shape: ' + str(ending_year_array.shape) + ' != ' + str(starting_year_array.shape) + '. This means that the coarse definition of the scenario that you are subtracting from the coarse definition of the baseline is mixing resolutions. You probably want to resample one of the two layers first.') 
                            current_array = (ending_year_array - starting_year_array) * ha_per_cell_array

                            hb.save_array_as_geotiff(current_array, current_ending_year_dst_path, p.aoi_ha_per_cell_coarse_path)

                        previous_year = year
                    

# def coarse_simplified_ha_difference_from_previous_year_ALT(p):
#     # Calculate LUH2_simplified difference with an alternative basemap for the first year after base year
#     task_documentation = """Calculates LUH2_simplified difference using a new basemap for the first year after the base year, then follows the standard process."""
    
#     if p.run_this:      
#         for index, row in p.scenarios_df.iterrows():
#             seals_utils.assign_df_row_to_object_attributes(p, row)
#             hb.log('Processing scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)) + ' with row ' + str([i for i in row]))

#             # Load land-use correspondence dictionary
#             if hb.path_exists(os.path.join(p.input_dir, p.coarse_correspondence_path)):
#                 p.lulc_correspondence_dict = hb.utils.get_reclassification_dict_from_df(
#                     os.path.join(p.input_dir, p.coarse_correspondence_path), 
#                     'src_id', 'dst_id', 'src_label', 'dst_label'
#                 )
#             elif hb.path_exists(os.path.join(p.base_data_dir, p.coarse_correspondence_path)):
#                 p.lulc_correspondence_dict = hb.utils.get_reclassification_dict_from_df(
#                     os.path.join(p.base_data_dir, p.coarse_correspondence_path), 
#                     'src_id', 'dst_id', 'src_label', 'dst_label'
#                 )
#             else:
#                 raise NameError('Unable to find ' + p.coarse_correspondence_path)

#             cell_size = hb.get_cell_size_from_path(p.ha_per_cell_coarse_path)
#             ha_per_cell_array = hb.as_array(p.aoi_ha_per_cell_coarse_path)

#             if p.scenario_type != 'baseline':
#                 for c, i in enumerate(p.lulc_correspondence_dict['dst_ids']):
#                     dst_class_label = p.lulc_correspondence_dict['dst_labels'][c]

#                     baseline_reference_label = row['baseline_reference_label']
#                     baseline_reference_row = p.scenarios_df.loc[p.scenarios_df['scenario_label'] == baseline_reference_label]
#                     baseline_exogenous_label = baseline_reference_row['exogenous_label'].values[0]
#                     baseline_reference_model = baseline_reference_row['model_label'].values[0]

#                     current_starting_year = None
#                     previous_year = None

#                     for year in p.years:
#                         if current_starting_year is None:
#                             base_year = int(row['key_base_year'])
#                             current_starting_year = base_year
#                             current_starting_year_dir = os.path.join(
#                                 p.coarse_simplified_proportion_dir, 
#                                 baseline_exogenous_label, 
#                                 baseline_reference_model, 
#                                 str(current_starting_year)
#                             )
#                             current_starting_year_path = os.path.join(
#                                 current_starting_year_dir, 
#                                 f"{dst_class_label}_prop_{baseline_exogenous_label}_{baseline_reference_model}_{current_starting_year}.tif"
#                             )
#                         else:
#                             current_starting_year = previous_year
#                             current_starting_year_dir = os.path.join(
#                                 p.coarse_simplified_proportion_dir, 
#                                 p.exogenous_label, 
#                                 p.climate_label, 
#                                 p.model_label, 
#                                 p.counterfactual_label, 
#                                 str(current_starting_year)
#                             )
#                             current_starting_year_path = os.path.join(
#                                 current_starting_year_dir, 
#                                 f"{dst_class_label}_prop_{p.exogenous_label}_{p.climate_label}_{p.model_label}_{p.counterfactual_label}_{current_starting_year}.tif"
#                             )

#                         current_ending_year_src_dir = os.path.join(
#                             p.coarse_simplified_proportion_dir, 
#                             p.exogenous_label, 
#                             p.climate_label, 
#                             p.model_label, 
#                             p.counterfactual_label, 
#                             str(year)
#                         )
#                         current_ending_year_src_path = os.path.join(
#                             current_ending_year_src_dir, 
#                             f"{dst_class_label}_prop_{p.exogenous_label}_{p.climate_label}_{p.model_label}_{p.counterfactual_label}_{year}.tif"
#                         )

#                         current_ending_year_dst_dir = os.path.join(
#                             p.cur_dir, 
#                             p.exogenous_label, 
#                             p.climate_label, 
#                             p.model_label, 
#                             p.counterfactual_label, 
#                             str(year)
#                         )
#                         hb.create_directories(current_ending_year_dst_dir)

#                         current_ending_year_dst_path = os.path.join(
#                             current_ending_year_dst_dir, 
#                             f"{dst_class_label}_{year}_{current_starting_year}_ha_diff_{p.exogenous_label}_{p.climate_label}_{p.model_label}_{p.counterfactual_label}.tif"
#                         )

#                         if not hb.path_exists(current_ending_year_dst_path):
#                             # Load ending year raster
#                             ending_year_array = hb.load_geotiff_chunk_by_bb(current_ending_year_src_path, p.bb)
#                             ending_year_ndv = hb.get_ndv_from_path(current_ending_year_src_path)

#                             # Use new basemap ONLY for the first year after the base year
#                             if hasattr(p, 'alt_base_lulc_path') and (year == base_year + 1):
#                                 if not hb.path_exists(p.alt_base_lulc_path):
#                                     raise NameError(f"New basemap file not found: {p.alt_base_lulc_path}")
#                                 hb.log(f"Using new basemap for year {year-1}")
#                                 # Uses bb for ESA, which results in mismatch
#                                 # Load entire alt basemap for now
#                                 # starting_year_array = hb.load_geotiff_chunk_by_bb(p.alt_base_lulc_path, p.bb)
#                                 alt_bb = hb.spatial_projection.get_bounding_box(p.alt_base_lulc_path)
#                                 starting_year_array = hb.load_geotiff_chunk_by_bb(p.alt_base_lulc_path, alt_bb)
#                                 starting_year_ndv = hb.get_ndv_from_path(p.alt_base_lulc_path)

#                                 # Mask alt basemap to the current class
#                                 if i not in np.unique(starting_year_array):
#                                     raise NameError(f"Class {i} not found in alt basemap")
#                                 starting_year_array = np.where(starting_year_array == i, 1, 0)
#                                 # NOTE: ending_year_array is in proportion (0-1), stating_year_array is binary from categorical (0/1)

#                                 # Ensure resolution consistency
#                                 if ending_year_array.shape != starting_year_array.shape:
#                                     hb.log(f"Shape mismatch: {ending_year_array.shape} != {starting_year_array.shape}.")
#                                     hb.log(f"Interpolating basemap array to match coarse resolution using 'nearest' method.")
#                                     def nearest_neighbor_resample(array, new_shape):
#                                         old_shape = array.shape
#                                         row_indices = np.linspace(0, old_shape[0] - 1, new_shape[0]).astype(int)
#                                         col_indices = np.linspace(0, old_shape[1] - 1, new_shape[1]).astype(int)
#                                         return array[np.ix_(row_indices, col_indices)]
#                                     starting_year_array = nearest_neighbor_resample(starting_year_array, ending_year_array.shape)
#                                     assert ending_year_array.shape == starting_year_array.shape

#                             else: # Use the standard basemap
#                                 starting_year_array = hb.load_geotiff_chunk_by_bb(current_starting_year_path, p.bb)
#                                 starting_year_ndv = hb.get_ndv_from_path(current_starting_year_path)

#                             # Handle NoData values
#                             ending_year_array = np.where(ending_year_array == ending_year_ndv, 0, ending_year_array)
#                             starting_year_array = np.where(starting_year_array == starting_year_ndv, 0, starting_year_array)

#                             # Compute land-use change difference
#                             current_array = (ending_year_array - starting_year_array) * ha_per_cell_array

#                             # Save output raster
#                             hb.save_array_as_geotiff(current_array, current_ending_year_dst_path, p.aoi_ha_per_cell_coarse_path)

#                         previous_year = year
