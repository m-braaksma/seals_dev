import os
import hazelbean as hb
import numpy as np
import pandas as pd
import logging
import pygeoprocessing
import csv
import seals_utils
from natcap.invest import pollination

L = hb.get_logger()

def pollination_sufficiency(p):
    """
    Generate pollination sufficiency from Land Use/Land Cover (LULC) data
    using NatCap's pollination module.
    
    - Input raster: LULC (categorical)
    - Output raster: Pollination sufficiency index (scale 0-1)
    """

    # Define projection in Mollweide (meters)
    wkt_projection = (
        'PROJCS["World_Mollweide",'
        'GEOGCS["GCS_WGS_1984",'
        'DATUM["WGS_1984",'
        'SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],'
        'UNIT["Degree",0.0174532925199433]],'
        'PROJECTION["Mollweide"],'
        'PARAMETER["False_Easting",0],'
        'PARAMETER["False_Northing",0],'
        'PARAMETER["Central_Meridian",0],'
        'UNIT["Meter",1]]'
    )

    for index, row in p.scenarios_df.iterrows():
        seals_utils.assign_df_row_to_object_attributes(p, row)
        hb.log('Running InVEST pollination module for scenario ' + str(index) + ' of ' + str(len(p.scenarios_df)))
        if p.scenario_type != 'baseline':
            for year in p.years:
                # Reproject SEALS
                lulc_stitched_name = 'lulc_' + p.lulc_src_label + '_' + p.lulc_simplification_label + '_' + p.exogenous_label + '_' + p.climate_label + '_' + p.model_label + '_' + p.counterfactual_label + '_' + str(year)
                lulc_stitched_name_path = os.path.join(p.stitched_lulc_simplified_scenarios_dir, lulc_stitched_name + '.tif')
                reprojected_lulc_stitched_name_path = os.path.join(p.cur_dir, lulc_stitched_name + '.tif')
                pygeoprocessing.warp_raster(
                    base_raster_path=lulc_stitched_name_path,
                    target_pixel_size=(300, -300),  # 300m resolution
                    target_raster_path=reprojected_lulc_stitched_name_path,
                    resample_method='nearest',
                    target_projection_wkt=wkt_projection,
                )

                # Define InVEST pollination model output directories
                pollination_workspace_dir = os.path.join(p.cur_dir, lulc_stitched_name)

                # Run InVEST pollination model for each scenario
                if not hb.path_exists(pollination_workspace_dir):
                    p.pollination_args['workspace_dir'] = pollination_workspace_dir
                    p.pollination_args['landcover_raster_path'] = reprojected_lulc_stitched_name_path
                    pollination.execute(p.pollination_args)

        # Repeat for baseline  
        # Reproject SEALS
        if hasattr(p, 'alt_base_lulc_path'):
            lulc_stitched_name_path = p.alt_base_lulc_path
            reprojected_lulc_stitched_name_path = os.path.join(p.cur_dir, 'alt_base_map.tif')
        else:
            lulc_stitched_name = 'lulc_' + p.lulc_src_label + '_' + p.lulc_simplification_label + '_' + str(p.base_years[0]) # take first base year in list
            lulc_stitched_name_path = os.path.join(p.fine_processed_inputs_dir, 'lulc', 'esa', 'seals7', lulc_stitched_name + '.tif')
            reprojected_lulc_stitched_name_path = os.path.join(p.cur_dir, lulc_stitched_name + '.tif')
        pygeoprocessing.warp_raster(
            base_raster_path=lulc_stitched_name_path,
            target_pixel_size=(300, -300),  # 300m resolution
            target_raster_path=reprojected_lulc_stitched_name_path,
            resample_method='nearest',
            target_projection_wkt=wkt_projection,
        )
        # Define InVEST pollination model output directories
        pollination_workspace_dir = os.path.join(p.cur_dir, lulc_stitched_name)
        # Run InVEST pollination model for each scenario
        if not hb.path_exists(pollination_workspace_dir):
            p.pollination_args['workspace_dir'] = pollination_workspace_dir
            p.pollination_args['landcover_raster_path'] = reprojected_lulc_stitched_name_path
            pollination.execute(p.pollination_args)


def calculate_pollinator_adjusted_value(lulc, poll_suff, crop_value_max_lost, crop_value_baseline, year, sufficient_pollination_threshold, L):
    """
    Compute crop value adjusted for pollination sufficiency.
    
    - Inputs:
        - lulc: Land Use/Land Cover raster (categorical)
        - poll_suff: Pollination sufficiency raster (scale 0-1)
        - crop_value_max_lost: Maximum economic loss due to pollination loss ($/ha)
        - crop_value_baseline: Baseline crop value ($/ha)
    - Output:
        - Adjusted crop value raster array ($/ha)
    """
    L.info(f'Calculating pollination-adjusted crop value for {year}')
    return np.where(
        (crop_value_max_lost > 0) & (poll_suff < sufficient_pollination_threshold) & (lulc == 2),
        crop_value_baseline - crop_value_max_lost * (1 - (1 / sufficient_pollination_threshold) * poll_suff),
        np.where(
            (crop_value_max_lost > 0) & (poll_suff >= sufficient_pollination_threshold) & (lulc == 2),
            crop_value_baseline,
            -9999.
        )
    )


def calculate_crop_value_and_shock(p):

    # Create output paths with unique filenames for each scenario
    # crop_value_difference_path = os.path.join(p.output_dir, 'pollination', 'crop_value', f'crop_value_difference_from_baseline_to_{p.scenario_label}_{p.year}.tif')
    crop_value_pollinator_adjusted_output_path = os.path.join(p.output_dir, 'pollination', 'crop_value', f'crop_value_pollinator_adjusted_{p.scenario_label}_{p.year}.tif')

    # Threshold for sufficient pollination
    sufficient_pollination_threshold = 0.3

    # Step 1: Process Crop Data
    # Load crop dependence data
    pollination_dependence_spreadsheet_input_path = os.path.join(p.base_data_dir, 'crops', 'rspb20141799supp3.xls')
    df_dependence = pd.read_excel(pollination_dependence_spreadsheet_input_path, sheet_name='Crop nutrient content')
    crop_names = list(df_dependence['Crop map file name'])[:-3]  # Removing the last 3 crops that don't have matching production data
    pollination_dependence = list(df_dependence['poll.dep'])

    # Initialize arrays for calculations
    ref_raster = os.path.join(p.crop_data_dir, 'production', 'alfalfa_HarvAreaYield_Geotiff', 'alfalfa_Production.tif')
    ha_shape = hb.get_shape_from_dataset_path(ref_raster)
    crop_value_baseline = np.zeros(ha_shape)
    crop_value_no_pollination = np.zeros(ha_shape)

    # Calculate baseline crop value and no-pollination value
    for c, crop_name in enumerate(crop_names):
        L.info(f'Calculating crop value for {crop_name} with pollination dependence {pollination_dependence[c]}')

        # Load crop production data
        crop_yield_path = os.path.join(p.base_data_dir, 'crops', 'production', f'{crop_name}_HarvAreaYield_Geotiff', f'{crop_name}_Production.tif')
        crop_yield = hb.as_array(crop_yield_path)
        crop_yield = np.where(crop_yield > 0, crop_yield, 0.0)

        # Calculate value (only based on production)
        crop_value_baseline += crop_yield
        crop_value_no_pollination += crop_yield * (1 - float(pollination_dependence[c]))

    # Calculate maximum loss due to pollination
    crop_value_max_lost = crop_value_baseline - crop_value_no_pollination

    # Save baseline and max loss
    crop_value_baseline_path = os.path.join(p.cur_dir, f'crop_value_baseline.tif')
    crop_value_max_lost_path = os.path.join(p.cur_dir, f'crop_value_max_lost.tif')
    hb.save_array_as_geotiff(crop_value_baseline, crop_value_baseline_path, ref_raster, ndv=-9999, data_type=6)
    hb.save_array_as_geotiff(crop_value_max_lost, crop_value_max_lost_path, ref_raster, ndv=-9999, data_type=6)

    # Step 2: Calculate Crop Value Adjusted for Pollination Sufficiency
    # Substeps:
    # Put all inside loop and check if reprojected exists
    # 1. Resample baseline rasters (crops, pollination)
    # 2. Resample scenario reasters (lulc scenario, pollination scenario)
    # Load arrays
    # Run function on baseline
    # Run function on scenarios
    # Convert to region level shock
    # Save

    # Loop over all scenarios (lulc scenario, pollination scenario)

    for index, row in p.scenarios_df.iterrows():
        seals_utils.assign_df_row_to_object_attributes(p, row)

        # Skip baseline scenario if necessary
        if p.scenario_type == 'baseline':
            continue  

        # Iterate over years for the given scenario
        for year in p.years:
            # Define crop paths
            resampled_crop_value_baseline_path = os.path.join(p.cur_dir, f'resampled_crop_value_baseline.tif')
            resampled_crop_value_max_lost_path = os.path.join(p.cur_dir, f'resampled_crop_value_max_lost.tif')

            # Define poll paths
            pollination_baseline_dir_name = f'lulc_{p.lulc_src_label}_{p.lulc_simplification_label}_{p.base_years[0]}'
            poll_suff_baseline_path = os.path.join(p.pollination_sufficiency_dir, pollination_baseline_dir_name, 'total_pollinator_abundance_spring.tif')
            resampled_poll_suff_baseline_path = os.path.join(p.cur_dir, f'poll_{pollination_baseline_dir_name}.tif')
            pollination_scenario_dir_name = f'lulc_{p.lulc_src_label}_{p.lulc_simplification_label}_{p.exogenous_label}_{p.climate_label}_{p.model_label}_{p.counterfactual_label}_{year}'
            poll_suff_scenario_path = os.path.join(p.pollination_sufficiency_dir, pollination_scenario_dir_name, 'total_pollinator_abundance_spring.tif')
            resampled_poll_suff_scenario_path = os.path.join(p.cur_dir, f'poll_{pollination_scenario_dir_name}.tif')

            # Define LULC paths
            if hasattr(p, 'alt_base_lulc_path'):
                lulc_baseline_path = os.path.join(p.cur_dir, 'alt_base_map.tif')
            else:
                lulc_name_baseline = 'lulc_' + p.lulc_src_label + '_' + p.lulc_simplification_label + '_' + str(p.base_years[0])
                lulc_baseline_path = os.path.join(p.fine_processed_inputs_dir, 'lulc', 'esa', 'seals7', lulc_name_baseline + '.tif')
                # reprojected_lulc_stitched_name_path = os.path.join(p.cur_dir, lulc_stitched_name + '.tif')
                # resampled_lulc_baseline_path = os.path.join(p.cur_dir, 'pollination', 'pollination_sufficiency', f'resampled_lulc_{p.scenario_label}_{p.base_year}.tif')
            lulc_stitched_name_scenario = f'lulc_{p.lulc_src_label}_{p.lulc_simplification_label}_{p.exogenous_label}_{p.climate_label}_{p.model_label}_{p.counterfactual_label}_{year}.tif'
            lulc_stitched_scenario_path = os.path.join(p.stitched_lulc_simplified_scenarios_dir, lulc_stitched_name_scenario)

            # Define input-output raster pairs for processing
            raster_paths = [
                (crop_value_baseline_path, resampled_crop_value_baseline_path),
                (crop_value_max_lost_path, resampled_crop_value_max_lost_path),
                (poll_suff_baseline_path, resampled_poll_suff_baseline_path),
                (poll_suff_scenario_path, resampled_poll_suff_scenario_path),
                # (lulc_baseline_path, resampled_lulc_baseline_path)
            ]

            # Resample raster files
            raster_info = pygeoprocessing.get_raster_info(lulc_baseline_path)
            target_pixel_size = raster_info['pixel_size']
            target_projection_wkt = raster_info['projection_wkt']
            target_bb = raster_info['bounding_box']
            for base_path, resampled_path in raster_paths:
                if not os.path.exists(resampled_path):
                    pygeoprocessing.warp_raster(
                        base_raster_path=base_path,
                        target_pixel_size=target_pixel_size,
                        target_raster_path=resampled_path,
                        resample_method='nearest',
                        target_projection_wkt=target_projection_wkt,
                        target_bb=target_bb,
                    )


            # Load rasters as arrays
            crop_value_max_lost = hb.as_array(resampled_crop_value_max_lost_path)
            crop_value_baseline = hb.as_array(resampled_crop_value_baseline_path)
            poll_suff_baseline = hb.as_array(resampled_poll_suff_baseline_path)
            poll_suff_scenario = hb.as_array(resampled_poll_suff_scenario_path)
            lulc_baseline = hb.as_array(lulc_baseline_path)
            lulc_scenario = hb.as_array(lulc_stitched_scenario_path)

            # Compute pollination-adjusted crop value for baseline
            crop_value_pollinator_adjusted_baseline = calculate_pollinator_adjusted_value(
                    lulc_baseline, poll_suff_baseline, crop_value_max_lost, crop_value_baseline, p.base_years[0], sufficient_pollination_threshold, L
            )
            baseline_output_path = os.path.join(p.cur_dir, f'crop_value_pollinator_adjusted_baseline.tif')
            hb.save_array_as_geotiff(crop_value_pollinator_adjusted_baseline, baseline_output_path, lulc_baseline_path, ndv=-9999, data_type=6)

            # Compute pollination-adjusted crop value for scenario
            crop_value_pollinator_adjusted_scenario = calculate_pollinator_adjusted_value(
                    lulc_scenario, poll_suff_scenario, crop_value_max_lost, crop_value_baseline, year, sufficient_pollination_threshold, L
            )
            scenario_output_path = os.path.join(p.cur_dir, f'crop_value_pollinator_adjusted_{p.exogenous_label}_{p.climate_label}_{p.model_label}_{p.counterfactual_label}_{year}.tif')
            hb.save_array_as_geotiff(crop_value_pollinator_adjusted_scenario, scenario_output_path, lulc_stitched_scenario_path, ndv=-9999, data_type=6)

            # Compute shock value by region
            L.info('Calculating shock value by region')
            region_array = hb.as_array(p.regions_path)
            unique_regions = np.unique(region_array)
            region_shocks = {}

            for region in unique_regions:
                if region in (-1, 255):  # Skip nodata values
                    continue

                mask = region_array == region
                scenario_sum = np.sum(crop_value_pollinator_adjusted_scenario[mask])
                baseline_sum = np.sum(crop_value_pollinator_adjusted_baseline[mask])

                shock_value = np.nan if baseline_sum == 0 else 1 - ((scenario_sum - baseline_sum) / baseline_sum)
                region_shocks[region] = shock_value

            # Save shock values to CSV
            shock_value_path = os.path.join(p.cur_dir, f'pollination_shock_{p.lulc_simplification_label}_{p.exogenous_label}_{p.climate_label}_{p.model_label}_{p.counterfactual_label}_{year}.csv')

            with open(shock_value_path, 'w', newline='') as shock_file:
                writer = csv.writer(shock_file)
                writer.writerow(['Region', 'Shock Value'])
                for region, value in region_shocks.items():
                    writer.writerow([region, value])
