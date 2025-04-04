import os, sys
import seals_utils
import seals_initialize_project
import hazelbean as hb
import pandas as pd

main = ''
if __name__ == '__main__':

    ### ------- ENVIRONMENT SETTINGS -------------------------------
    # Users should only need to edit lines in this section
    
    # Create a ProjectFlow Object to organize directories and enable parallel processing.
    p = hb.ProjectFlow()

    # Assign project-level attributes to the p object (such as in p.base_data_dir = ... below)
    # including where the project_dir and base_data are located.
    # The project_name is used to name the project directory below. If the directory exists, each task will not recreate
    # files that already exist. 
    p.user_dir = os.path.expanduser('~')        
    p.extra_dirs = ['Files', 'seals', 'projects']
    p.project_name = 'run_ken_poll_shock'
    # p.project_name = p.project_name + '_' + hb.pretty_time() # If don't you want to recreate everything each time, comment out this line.
    
    # Based on the paths above, set the project_dir. All files will be created in this directory.
    p.project_dir = os.path.join(p.user_dir, os.sep.join(p.extra_dirs), p.project_name)
    p.set_project_dir(p.project_dir) 
    
    p.run_in_parallel = 1 # Must be set before building the task tree if the task tree has parralel iterator tasks.

    # Build the task tree via a building function and assign it to p. IF YOU WANT TO LOOK AT THE MODEL LOGIC, INSPECT THIS FUNCTION
    seals_initialize_project.build_ken_task_tree(p)

    # Set the base data dir. The model will check here to see if it has everything it needs to run.
    # If anything is missing, it will download it. You can use the same base_data dir across multiple projects.
    # Additionally, if you're clever, you can move files generated in your tasks to the right base_data_dir
    # directory so that they are available for future projects and avoids redundant processing.
    # The final directory has to be named base_data to match the naming convention on the google cloud bucket.
    p.base_data_dir = os.path.join(p.user_dir, 'Files/base_data')

    # ProjectFlow downloads all files automatically via the p.get_path() function. If you want it to download from a different 
    # bucket than default, provide the name and credentials here. Otherwise uses default public data 'gtap_invest_seals_2023_04_21'.
    p.data_credentials_path = None
    p.input_bucket_name = None
    
    ## Set defaults and generate the scenario_definitions.csv if it doesn't exist.
    # SEALS will run based on the scenarios defined in a scenario_definitions.csv
    # If you have not run SEALS before, SEALS will generate it in your project's input_dir.
    # A useful way to get started is to to run SEALS on the test data without modification
    # and then edit the scenario_definitions.csv to your project needs.   
    p.scenario_definitions_filename = 'test_standard_scenarios.csv' 
    p.scenario_definitions_path = os.path.join(p.input_dir, p.scenario_definitions_filename)
    seals_initialize_project.initialize_scenario_definitions(p)
        
    # SEALS is based on an extremely comprehensive region classification system defined in the following geopackage.
    global_regions_vector_ref_path = os.path.join('cartographic', 'ee', 'ee_r264_correspondence.gpkg')
    p.global_regions_vector_path = p.get_path(global_regions_vector_ref_path)

    # Set processing resolution: determines how large of a chunk should be processed at a time. 4 deg is about max for 64gb memory systems
    p.processing_resolution = 1.0 # In degrees. Must be in pyramid_compatible_resolutions

    seals_initialize_project.set_advanced_options(p)

    # Explicitly set pollination attributes for testing/dev
    p.pollination_args = {
    'farm_vector_path': '',
    'guild_table_path': '/Users/mbraaksma/Files/base_data/global_invest/pollination/guild_table.csv',
    'landcover_biophysical_table_path': '/Users/mbraaksma/Files/base_data/global_invest/pollination/landcover_biophysical_table.csv',
    'landcover_raster_path': '',
    'n_workers': '-1',
    'results_suffix': '',
    'workspace_dir': '',
    }
    # p.output_dir = '/Users/mbraaksma/Files/seals/projects/run_ken_poll_shock/es_models'
    # p.lulc_baseline_path = '/Users/mbraaksma/Files/seals/projects/run_ken_poll_shock/intermediate/fine_processed_inputs/lulc/esa/seals7/lulc_esa_seals7_2017.tif'
    # p.lulc_scenario_path = '/Users/mbraaksma/Files/seals/projects/run_ken_poll_shock/intermediate/stitched_lulc_simplified_scenarios/lulc_esa_seals7_ssp2_rcp45_luh2-message_bau_2030.tif'
    p.regions_path = '/Users/mbraaksma/Files/base_data/demetra-seals/borders/kenya_aez_300m.tif'
    p.crop_data_dir = '/Users/mbraaksma/Files/base_data/crops'
    p.pollination_dependence_spreadsheet_input_path = '/Users/mbraaksma/Files/base_data/crops/rspb20141799supp3.xls' # Note had to fix pol.dep for cofee and greenbroadbean as it was 25 not .25
    # p.output_dir = '/Users/mbraaksma/Files/seals/projects/run_ken_poll_shock/es_models'
    # p.scenario_label = 'ssp2_rcp45'
    # p.year = 2030
    # p.base_year = 2017
    # p.poll_suff_baseline_path = f'/Users/mbraaksma/Files/seals/projects/run_ken_poll_shock/es_models/pollination/pollination_sufficiency/{p.scenario_label}_luh2-message_bau_{p.base_year}/total_pollinator_abundance_spring.tif'
    # p.poll_suff_scenario_path = f'/Users/mbraaksma/Files/seals/projects/run_ken_poll_shock/es_models/pollination/pollination_sufficiency/{p.scenario_label}_luh2-message_bau_{p.year}/total_pollinator_abundance_summer.tif'
    
    p.L = hb.get_logger('test_run_seals')
    hb.log('Created ProjectFlow object at ' + p.project_dir + '\n    from script ' + p.calling_script + '\n    with base_data set at ' + p.base_data_dir)
    
    p.execute()

    result = 'Done!'

