import os, sys
import seals_utils
import seals_initialize_project
import hazelbean as hb
import pandas as pd
import pygeoprocessing as pg
import geopandas as gpd
import numpy as np

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
    p.project_name = 'run_ken_adjust_base_lulc'
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
    p.scenario_definitions_filename = 'ken_scenarios.csv' 
    p.scenario_definitions_path = os.path.join(p.input_dir, p.scenario_definitions_filename)
    seals_initialize_project.initialize_scenario_definitions(p)
        
    # SEALS is based on an extremely comprehensive region classification system defined in the following geopackage.
    global_regions_vector_ref_path = os.path.join('cartographic', 'ee', 'ee_r264_correspondence.gpkg')
    p.global_regions_vector_path = p.get_path(global_regions_vector_ref_path)

    # Set processing resolution: determines how large of a chunk should be processed at a time. 4 deg is about max for 64gb memory systems
    p.processing_resolution = 1.0 # In degrees. Must be in pyramid_compatible_resolutions

    # Change base LULC map
    p.alt_base_lulc_path = '/Users/mbraaksma/Files/base_data/demetra-seals/lulc/stakeholder2seals/2020_composite_seals7.tif'
    # p.alt_base_lulc_path = os.path.join(p.base_data_dir, 'lulc', 'stakeholder2seals', '2020_composite_seals7.tif')

    seals_initialize_project.set_advanced_options(p)
    
    p.L = hb.get_logger('test_run_seals')
    hb.log('Created ProjectFlow object at ' + p.project_dir + '\n    from script ' + p.calling_script + '\n    with base_data set at ' + p.base_data_dir)
    
    p.execute()

    result = 'Done!'


    # Add zonal statistics step
    def lulc_zonal_statistics(
    raster_path, vector_path, rasterized_vector_path, zone_field, 
    multiplier_raster_path=None, zone_labels=None, lulc_labels=None
    ):
        """Calculate zonal statistics for categorical LULC data using pygeoprocessing.

        Args:
            raster_path (str): Path to the input raster file.
            vector_path (str): Path to the vector file containing zone polygons.
            rasterized_vector_path (str): Path for the rasterized vector output.
            zone_field (str): Attribute field in vector containing zone identifiers.
            multiplier_raster_path (str, optional): Path to a raster with per-pixel multipliers (e.g., hectare values). Defaults to None.
            zone_labels (dict, optional): Mapping of zone IDs to labels. Defaults to None.
            lulc_labels (dict, optional): Mapping of LULC class IDs to labels. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame with zone names and LULC class areas as columns.
        """
    
        # Get raster metadata
        raster_info = pg.get_raster_info(raster_path)

        # Create rasterized output path
        if not os.path.exists(rasterized_vector_path):
            print('Rasterizing vector zones...')
            pg.create_raster_from_bounding_box(
                target_bounding_box=raster_info['bounding_box'],
                target_raster_path=rasterized_vector_path,
                target_pixel_size=raster_info['pixel_size'],
                target_pixel_type=raster_info['datatype'],
                target_srs_wkt=raster_info['projection_wkt'],
                target_nodata=raster_info['nodata'][0]
            )

            # Rasterize vector zones
            pg.rasterize(
                vector_path, rasterized_vector_path, option_list=["ATTRIBUTE=" + zone_field]
            )

        # Load raster data
        raster_array = pg.raster_to_numpy_array(raster_path)
        zone_array = pg.raster_to_numpy_array(rasterized_vector_path)
        multiplier_array = None

        if multiplier_raster_path:
            multiplier_array = pg.raster_to_numpy_array(multiplier_raster_path)

        # Get unique zones and LULC classes
        unique_zones = np.unique(zone_array)
        unique_zones = unique_zones[(unique_zones != 0) & (unique_zones != 255)]  # Exclude background/ndv
        unique_classes = np.unique(raster_array)
        unique_classes = unique_classes[(unique_classes != 0) & (unique_classes != 255)]  # Exclude background/ndv
            # Check if the number of unique zones matches the zone_labels provided
        if zone_labels:
            missing_zones = set(unique_zones) - set(zone_labels.keys())
            extra_zones = set(zone_labels.keys()) - set(unique_zones)

            if missing_zones:
                print(f"Warning: The following zones are in the raster but missing from zone_labels: {missing_zones}")
            if extra_zones:
                print(f"Warning: The following zones are in zone_labels but not found in the raster data: {extra_zones}")


        # Initialize dictionary to store results
        results = {}

        for zone in unique_zones:
            zone_label = zone_labels.get(zone, zone) if zone_labels else zone
            zone_mask = zone_array == zone

            # Initialize row for this zone
            if zone_label not in results:
                results[zone_label] = {"region": zone_label}
                results[zone_label]["zone_id"] = zone

            for lulc_class in unique_classes:
                lulc_label = lulc_labels.get(lulc_class, f"class_{lulc_class}") if lulc_labels else f"class_{lulc_class}"
                class_mask = (raster_array == lulc_class) & zone_mask
                count = np.sum(class_mask)

                if multiplier_array is not None:
                    area = np.sum(multiplier_array[class_mask])
                else:
                    area = count  # Default to raw count if no multiplier is provided

                results[zone_label][f"{lulc_label}_ha"] = area

        # Convert dictionary to DataFrame
        df = pd.DataFrame.from_dict(results, orient="index").reset_index(drop=True)

        return df

    year_list = range(2020, 2051)
    all_years_data = []

    for year in year_list:
        print(f"Processing year: {year}")
        # Determine raster file path based on the year
        if year == 2020:
            input_raster_path = p.alt_base_lulc_path
        else:
            input_raster_path = f'/Users/mbraaksma/Files/seals/projects/run_ken_adjust_base_lulc/intermediate/stitched_lulc_simplified_scenarios/lulc_esa_seals7_ssp2_rcp45_luh2-message_bau_{year}.tif'
        # Paths and labels
        zones_vector_path = '/Users/mbraaksma/Files/base_data/demetra-seals/borders/kenya_aez.gpkg'
        rasterized_zones_vector_path = '/Users/mbraaksma/Files/base_data/demetra-seals/borders/kenya_aez_300m.tif'
        multiplier_raster_path = p.aoi_ha_per_cell_fine_path
        # multiplier_raster_path = '/Users/mbraaksma/Files/seals/projects/run_ken_2020_2050/intermediate/project_aoi/pyramids/aoi_ha_per_cell_fine.tif'
        zone_labels = {1: 'AN', 2: 'AS', 3: 'CO', 4: 'HR', 5: 'MN', 6: 'MO', 7: 'MS', 8: 'NA'}
        lulc_labels = {
            1: 'urban', 
            2: 'cropland', 
            3: 'grassland',
            4: 'forest', 
            5: 'othernat',
            6: 'water',
            7: 'other'
        }
        # Run zonal statistics
        df = lulc_zonal_statistics(
            raster_path=input_raster_path, 
            vector_path=zones_vector_path, 
            rasterized_vector_path=rasterized_zones_vector_path, 
            zone_field='generated_ids',
            multiplier_raster_path=multiplier_raster_path,
            zone_labels=zone_labels,
            lulc_labels=lulc_labels
        )
        df['year'] = year
        all_years_data.append(df)
    # Combine all years into a single DataFrame for this scenario
    panel_df = pd.concat(all_years_data, ignore_index=True)
    # Add percent cols
    lulc_ha_cols = [col for col in panel_df.columns if col.endswith('_ha')]
    panel_df['total_ha'] = panel_df[lulc_ha_cols].sum(axis=1)
    for col in lulc_ha_cols:
        panel_df[col.replace('_ha', '_percent')] = (panel_df[col] / panel_df['total_ha']) * 100
    # Reorder columns
    new_order = ['region', 'zone_id', 'year'] + [col for col in panel_df.columns if col not in ['region', 'zone_id', 'year']]
    panel_df = panel_df[new_order]

    # Save results for each scenario separately
    csv_output_path = f'/Users/mbraaksma/Files/base_data/demetra-seals/lulc/luh2/class_area_aez/lulc_stakeholder_seals7_ssp2_rcp45_luh2-message_bau_2020_2050.csv'
    panel_df.to_csv(csv_output_path, index=False)
    print(f"Saved results to: {csv_output_path}")
    # Plotting code
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    # Load the CSV
    df = pd.read_csv(csv_output_path, na_filter=False)
    df.drop(columns='total_ha', inplace=True)
    # Identify relevant columns
    lulc_columns = [col for col in df.columns if col.endswith('_ha')]
    region_column = 'region'  # Adjust if needed
    year_column = 'year'
    # Function to prepare stacked area data
    def prepare_stacked_area_data(df, year_col, lulc_cols):
        df = df.sort_values(by=year_col)
        return df[lulc_cols].T  # Transpose for stackplot
    # Define color scheme
    colors = {
        'urban': '#FF0000',      # Red
        'water': '#47BDFC',      # Pale Blue
        'cropland': '#FFD700',   # Yellowish
        'grassland': '#FFA500',  # Orange
        'forest': '#008000',     # Green
        'othernat': '#8B4513',   # Brown
        'other': '#808080'       # Grey
    }
    # Formatter function for y-axis
    def format_y_axis(value, _):
        return f'{int(value):,}'
    # Get unique regions
    regions = df[region_column].unique()
    num_regions = len(regions)
    # Create figure with a single column of plots (one per region + one for the country)
    fig, axes = plt.subplots(num_regions + 1, 1, figsize=(10, 5 * (num_regions + 1)), sharex=True)
    # Function to plot LULC trends
    def plot_lulc(ax, data, year_col, lulc_cols, title):
        stacked_data = prepare_stacked_area_data(data, year_col, lulc_cols)
        ax.stackplot(data[year_col], *stacked_data.values, 
                     colors=[colors[col.replace('_ha', '').lower()] for col in lulc_cols], alpha=0.7)
        ax.set_title(title)
        ax.set_xlabel('Year')
        ax.set_ylabel('Area (ha)')
        ax.legend(labels=[col.replace('_ha', '').capitalize() for col in lulc_cols], loc='upper left', fontsize='small')
        ax.set_xlim(2020, 2050)
        ax.yaxis.set_major_formatter(FuncFormatter(format_y_axis))
    # Plot overall country data
    country_data = df.groupby(year_column).sum().reset_index()
    plot_lulc(axes[0], country_data, year_column, lulc_columns, 'Overall Country LULC Change (2020-2050)')
    # Plot for each region
    for i, region in enumerate(regions):
        region_data = df[df[region_column] == region]
        plot_lulc(axes[i + 1], region_data, year_column, lulc_columns, f'LULC Change - {region} (2020-2050)')
    # Adjust layout and save
    plt.tight_layout()
    save_path = f'/Users/mbraaksma/Files/base_data/demetra-seals/lulc/luh2/class_area_aez/lulc_stakeholder_trends_plot.png'
    plt.savefig(save_path, dpi=300, bbox_inches="tight")
    print(f"Saved results to: {save_path}")


