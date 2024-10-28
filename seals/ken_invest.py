"""
This script executes several InVEST models (Carbon and Pollination) using land cover scenarios 
generated for Kenya. It reprojects land cover data to the required coordinate system and runs 
InVEST modules for each scenario and year.

Dependencies:
- GDAL (for raster operations)
- InVEST (Carbon, Pollination models)
- Logging for output tracking

Workflow:
1. Define paths for user directories, base data, and scenario data.
2. Reproject land cover data to ESRI:54030 coordinate system.
3. Run InVEST Carbon and Pollination models for specified scenarios and years.
4. Output is saved in designated directories for each model.

Note: This script assumes specific file structures for the inputs (e.g., SEALS LULC data) 
and will create necessary output directories if they don't exist.

Author: Matt Braaksma
Date: October 2024
"""

import logging
import sys
import os
import matplotlib.pyplot as plt 
import pygeoprocessing as pygeo
import numpy as np
from seals_visualization_functions import plot_array_as_seals7_lulc
import hazelbean as hb
from osgeo import gdal

import natcap.invest.carbon
import natcap.invest.pollination
import natcap.invest.annual_water_yield
import natcap.invest.ndr.ndr
import natcap.invest.sdr.sdr
import natcap.invest.utils

LOGGER = logging.getLogger(__name__)
root_logger = logging.getLogger()

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    fmt=natcap.invest.utils.LOG_FMT,
    datefmt='%m/%d/%Y %H:%M:%S ')
handler.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[handler])


args_carbon = {
    'calc_sequestration': False,
    'carbon_pools_path': '/Users/mbraaksma/Files/base_data/invest/global_invest/carbon/seals_biophysical_table.csv',
    'discount_rate': '',
    'do_redd': False,
    'do_valuation': False,
    'lulc_cur_path': '',
    'lulc_cur_year': '',
    'lulc_fut_path': '',
    'lulc_fut_year': '',
    'lulc_redd_path': '',
    'n_workers': '-1',
    'price_per_metric_ton_of_c': '',
    'rate_change': '',
    'results_suffix': '',
    'workspace_dir': '',
}

pollination_args = {
    'farm_vector_path': '',
    'guild_table_path': '/Users/mbraaksma/Files/base_data/invest/global_invest/pollination/guild_table.csv',
    'landcover_biophysical_table_path': '/Users/mbraaksma/Files/base_data/invest/global_invest/pollination/landcover_biophysical_table.csv',
    'landcover_raster_path': '',
    'n_workers': '-1',
    'results_suffix': '',
    'workspace_dir': '',
}

def generate_plot(cur_dir, scenario, year, model, file_name, unit):
    # Create a figure and axes for the grid
    plots_path = os.path.join(cur_dir, 'plots')
    raster_path = os.path.join(cur_dir, f'{model}', f'{scenario}_{year}', file_name+'.tif')
    raster_array = pygeo.raster_to_numpy_array(raster_path)
    raster_array[raster_array <= 0] = np.nan
    raster_array[raster_array > 1000000000] = np.nan
    plt.imshow(raster_array, cmap='viridis')  # You can change the colormap if needed
    plt.colorbar(label=f'{unit}')
    plt.title(f'{model} '+scenario+' luh2-message bau '+year)
    plt.tick_params(
        axis='both',       # Apply settings to both x and y axes
        which='both',      # Apply to both major and minor ticks
        bottom=False,      # Remove ticks on the bottom axis
        left=False,        # Remove ticks on the left axis
        labelbottom=False, # Remove tick labels on the bottom axis
        labelleft=False    # Remove tick labels on the left axis
    )
    combined_png_path = os.path.join(plots_path, file_name+f'_{scenario}_{year}.png')
    print(combined_png_path)
    plt.savefig(combined_png_path, format="png")
    plt.close()


if __name__ == '__main__':
    # DEFINE PATHS
    user_dir = os.path.expanduser('~')
    base_data = os.path.join(user_dir, 'Files', 'base_data', 'invest', 'global_invest')
    seals_data = os.path.join(user_dir, os.sep.join(['Files', 'seals', 'projects', 'run_ken_regional_projections','intermediate','stitched_lulc_simplified_scenarios']))
    cur_dir = os.path.join(user_dir, os.sep.join(['Files', 'seals', 'projects', 'run_ken_invest']))
    # kenya_vector_path = '/Users/mbraaksma/Library/CloudStorage/GoogleDrive-braak014@umn.edu/Shared drives/NatCapTEEMs/Projects/JRC Project/Data/borders/kenya_aez.gpkg'
    # Ensure the output directory exists
    os.makedirs(cur_dir, exist_ok=True)
    
    # DEFINE SCENARIOS
    scenario_list = ['ssp2_rcp45']
    year_list = [2045, 2075]

    # REPROJ SEALS
    for scenario in scenario_list:
        for year in year_list:
            input_raster = os.path.join(seals_data, f'lulc_esa_seals7_{scenario}_luh2-message_bau_{year}.tif')
            output_raster= os.path.join(seals_data, 'reproj_invest', f'lulc_esa_seals7_{scenario}_luh2-message_bau_{year}.tif')
            gdal.Warp(output_raster, input_raster, dstSRS='ESRI:54030')


    # RUN INVEST MODULES FOR EACH SCENARIO
    for scenario in scenario_list:
        for year in year_list:
            # CARBON
            print(scenario, year, 'carbon')
            output_dir = os.path.join(cur_dir, 'carbon', f'{scenario}_{year}')
            os.makedirs(output_dir, exist_ok=True)
            args_carbon['workspace_dir'] = output_dir
            args_carbon['lulc_cur_path'] = os.path.join(seals_data, 'reproj_invest', f'lulc_esa_seals7_{scenario}_luh2-message_bau_{year}.tif')
            if not os.path.exists(output_dir):
                natcap.invest.carbon.execute(args_carbon)
            generate_plot(cur_dir, f'{scenario}', f'{year}', 'carbon', 'tot_c_cur', 'metric tons of carbon stored per pixel')

            # POLLINATION
            print(scenario, year, 'pollination')
            output_dir = os.path.join(cur_dir, 'pollination', f'{scenario}_{year}')
            os.makedirs(output_dir, exist_ok=True)
            pollination_args['workspace_dir'] = output_dir
            pollination_args['landcover_raster_path'] = os.path.join(seals_data, 'reproj_invest', f'lulc_esa_seals7_{scenario}_luh2-message_bau_{year}.tif')
            if not os.path.exists(output_dir):
                natcap.invest.pollination.execute(pollination_args)
            generate_plot(cur_dir, f'{scenario}', f'{year}', 'pollination', 'total_pollinator_abundance_spring','total pollinator abundance across all species per pixel')



