import os
import hazelbean as hb
import pandas as pd


def assign_defaults_from_model_spec(input_object, model_spec_dict):
    # Helper function that takes an input object, like a ProjectFlow p variable
    # and a dictionary of default values. If the attribute is not already set, it will
    # set the attribute to the default value.    
    for k, v in model_spec_dict.items():
        if not hasattr(input_object, k):
            setattr(input_object, k, v)
            
# Make this follow model spec for all pre-processing and validation
def assign_df_row_to_object_attributes(input_object, input_row):
    # srtip() 
    # Rules: 
    # First check if is numeric
    # Then check if has extension, is path
    for attribute_name, attribute_value in list(zip(input_row.index, input_row.values)):
  
        try: 
            float(attribute_value)
            is_floatable = True
        except:
            is_floatable = False
        try:
            int(attribute_value)
            is_intable = True
        except:
            is_intable = False
        
        if attribute_name == 'calibration_parameters_source':
            pass
        # NOTE Clever use of p.get_path() here.
        if '.' in str(attribute_value) and not is_floatable: # Might be a path            
            path = input_object.get_path(attribute_value)
            setattr(input_object, attribute_name, path)
            
        elif 'year' in attribute_name:
            if ' ' in str(attribute_value):
                new_attribute_value = []
                for i in attribute_value.split(' '):
                    try:
                        new_attribute_value.append(int(i))
                    except:
                        new_attribute_value.append(str(i))
                attribute_value = new_attribute_value

                # attribute_value = [int(i) if 'nan' not in str(i) and intable else None for i in attribute_value.split(' ')]  
            elif is_intable:
                if attribute_name == 'key_base_year':
                    attribute_value = int(attribute_value)
                else:
                    attribute_value = [int(attribute_value)]
            elif 'lulc' in attribute_name: #
                attribute_value = str(attribute_value)
            else:
                if 'nan' not in str(attribute_value):
                    try:
                        attribute_value = [int(attribute_value)]
                    except:
                        attribute_value = [str(attribute_value)]
                else:
                    attribute_value = None
            setattr(input_object, attribute_name, attribute_value)

        elif 'dimensions' in attribute_name:
            if ' ' in str(attribute_value):
                attribute_value = [str(i) if 'nan' not in str(i) else None for i in attribute_value.split(' ')]  
            else:
                if 'nan' not in str(attribute_value):
                    attribute_value = [str(attribute_value)]
                else:
                    attribute_value = None
                  
            setattr(input_object, attribute_name, attribute_value)
        else:
            if str(attribute_value).lower() == 'nan':
                attribute_value = None
                setattr(input_object, attribute_name, attribute_value)
            else:
                # Check if the t string
                setattr(input_object, attribute_name, attribute_value)
                
                
                
    # UP NEXT: replicate the below 3 lines but referencing model spec. Also remember to add the regional_projections allocation algorithm input to the model spec. 
                
    model_spec = {}
    model_spec['regional_projections_input_path'] = ''
    assign_defaults_from_model_spec(input_object, model_spec)


# Old approach. Literally writes a csv.
def generate_scenarios_csv_and_put_in_input_dir(p):
    # In the event that the scenarios csv was not set, this currently wouldn't yet be a scenarios_df
    # Yet, I still want to be able to iterate over it. So thus, I need to GENERATE the scenarios_df from the project_flow
    # attributes
    list_of_attributes_to_write = [
        	
        'scenario_label',	
        'scenario_type',	
        'aoi',	
        'exogenous_label',	
        'climate_label',	
        'model_label',	
        'counterfactual_label',	
        'years',	
        'baseline_reference_label',	
        'base_years',	
        'key_base_year',
        'comparison_counterfactual_labels',	
        'time_dim_adjustment',	
        'coarse_projections_input_path',	
        'lulc_src_label',	
        'lulc_simplification_label',	
        'lulc_correspondence_path',	
        'coarse_src_label',	
        'coarse_simplification_label',	
        'coarse_correspondence_path',	
        'lc_class_varname',	
        'dimensions',	
        'calibration_parameters_source',	
        'base_year_lulc_path',
    ]


    data = {i: [] for i in list_of_attributes_to_write}

    # Add a baseline row. For the next scenario specific row it will actually take from the p attributes
    # however for the baseline row we have to override a few things (like setting years to the base years)
    data['scenario_label'].append('baseline_' + p.model_label)
    data['scenario_type'].append('baseline')
    data['aoi'].append(p.aoi)
    data['exogenous_label'].append('baseline')
    data['climate_label'].append(''	)
    data['model_label'].append(p.model_label)
    data['counterfactual_label'].append('')
    data['years'].append(' '.join([str(int(i)) for i in p.base_years]))
    data['baseline_reference_label'].append('' )
    data['base_years'].append(' '.join([str(int(i)) for i in p.base_years]))
    data['key_base_year'].append(p.key_base_year)
    data['comparison_counterfactual_labels'].append('')
    data['time_dim_adjustment'].append('add2015')
    data['coarse_projections_input_path'].append(p.coarse_projections_input_path)
    data['lulc_src_label'].append('esa')
    data['lulc_simplification_label'].append('seals7')
    data['lulc_correspondence_path'].append('seals/default_inputs/esa_seals7_correspondence.csv')
    data['coarse_src_label'].append('luh2-14')
    data['coarse_simplification_label'].append('seals7')
    data['coarse_correspondence_path'].append('seals/default_inputs/luh2-14_seals7_correspondence.csv')
    data['lc_class_varname'].append('all_variables')
    data['dimensions'].append('time')
    data['calibration_parameters_source'].append('seals/default_inputs/default_global_coefficients.csv')
    data['base_year_lulc_path'].append(p.base_year_lulc_path)


    # Add non baseline. Now that the baseline was added, we can now just iterate over the existing attributes
    for i in list_of_attributes_to_write:
        current_attribute = getattr(p, i)
        if type(current_attribute) is str:
            if current_attribute.startswith('['):
                current_attribute = ' '.join(list(current_attribute))
            elif os.path.isabs(current_attribute):
                current_attribute = hb.path_split_at_dir(current_attribute, os.path.split(p.base_data_dir)[1])[2].replace('\\\\', '\\').replace('\\', '/') # NOTE Awkward hack of assuming there is only 1 dir with the same name as base_data_dir

        elif type(current_attribute) is list:
            current_attribute = ' '.join([str(i) for i in current_attribute])

        data[i].append(current_attribute)

    p.scenarios_df = pd.DataFrame(data=data, columns=list_of_attributes_to_write)

    hb.create_directories(p.scenario_definitions_path)
    p.scenarios_df.to_csv(p.scenario_definitions_path, index=False)




# Old approach. Now we want to replace this with model spec, but here is a bunch of stuff that has modelspec info in comments
def set_attributes_to_default(p):
    # Set all ProjectFlow attributes to SEALS default.
    # This is used if the user has never run something before, and therefore doesn't
    # have a scenario_definitions.csv in their input dir.
    # This function will set the attributes, and can be paired with 
    # generate_scenarios_csv_and_put_in_input_dir to write the file.

    ###--- SET DEFAULTS ---###
    
    # String that uniquely identifies the scenario. Will be referenced by other scenarios for comparison.
    p.scenario_label = 'ssp2_rcp45_luh2-globio_bau'

    # Scenario type determines if it is historical (baseline) or future (anything else) as well
    # as what the scenario should be compared against. I.e., Policy minus BAU.
    p.scenario_type = 'bau'

    # Sets the area of interest. If set as a country-ISO3 code, all data will be generated based
    # that countries boundaries (as defined in the base data). Other options include setting it to
    # 'global' or a specific shapefile, or iso3 code. Good small examples include RWA, BTN
    p.aoi = 'RWA'

    # Exogenous label references some set of exogenous drivers like population, TFP growth, LUH2 pattern, SSP database etc
    p.exogenous_label = 'ssp2'

    # One of the climate RCPs
    p.climate_label = 'rcp45'

    # Indicator of which model led to the coarse projection
    p.model_label = 'luh2-message'

    # AKA policy scenario, or a label of something that you have tweaked to assess it's efficacy
    p.counterfactual_label = 'bau'

    # If is not a baseline scenario, these are years into the future. Duplicate the base_year variable below if it is a base year
    # From the csv, this is a space-separated list.
    p.years = [2100]

    # For calculating difference from base year, this references which baseline (must be consistent due to different
    # models having different depictions of the base year)
    p.baseline_reference_label = 'baseline_luh2-message'

    # Which year in the observed data constitutes the base year. There may be multiple if, for instance, you
    # want to use seals to downscale model outputs that update a base year to a more recent year base year
    # which would now be based on model results but is for an existing year. Paper Idea: Do this for validation.
    p.base_years = [2017]

    # Even with multiple years, we will designate one as the key base year, which will be used e.g.
    # for determining the resolution, extent, projection and other attributes of the fine resolution
    # lulc data. It must refer to a year that has observed LULC data to load.
    p.key_base_year = 2017


    # If set to one of the other policies, like BAU, this will indicate which scenario to compare the performance
    # of this scenario to. The tag 'no_policy' indicates that it should not be compared to anything.
    p.comparison_counterfactual_labels = 'no_policy'

    # Path to the coarse land-use change data. SEALS supports 2 types: a netcdf directory of geotiffs following
    # the documented structure.
    p.coarse_projections_input_path = "luh2/raw_data/rcp45_ssp2/multiple-states_input4MIPs_landState_ScenarioMIP_UofMD-MESSAGE-ssp245-2-1-f_gn_2015-2100.nc"
    # p.coarse_projections_input_path = 'luh2/raw_data/rcp26_ssp1/multiple-states_input4MIPs_landState_ScenarioMIP_UofMD-IMAGE-ssp126-2-1-f_gn_2015-2100.nc'

    # Label of the LULC data being reclassified into the simplified form.
    p.lulc_src_label = 'esa'

    # Label of the new LULC simplified classification
    p.lulc_simplification_label = 'seals7'

    # Path to a csv that will map the a many-to-one reclassification of
    # the src LULC map to a simplified version
    # includes at least src_id, dst_id, src_label, dst_label
    p.lulc_correspondence_path = 'seals/default_inputs/esa_seals7_correspondence.csv'

    # Label of the coarse LUC data that will be reclassified and downscaled
    p.coarse_src_label = 'luh2-14'

    # Label of the new coarse LUC data
    p.coarse_simplification_label = 'seals7'

    # Path to a csv that includes at least src_id, dst_id, src_label, dst_label
    p.coarse_correspondence_path = 'seals/default_inputs/luh2-14_seals7_correspondence.csv'

    # Often NetCDF files can have the time dimension in something other than just the year. This string allows
    # for doing operations on the time dimension to match what is desired. e.g., multiply5 add2015
    p.time_dim_adjustment = 'add2015'

    # Because different NetCDF files have different arrangements (e.g. time is in the dimension
    # versus LU_class is in the dimension), this option allows you to specify where in the input
    # NC the information is. If 'all_variables', assumes the LU classes will be the different variables named
    # otherwise it can be a subset of variables, otherwise, if it is a named variable, e.g. LC_area_share
    # then assume that the lc_class variable is stored in the last-listed dimension (see p.dimensions)
    p.lc_class_varname = 'all_variables'

    # Lists which dimensions are stored in the netcdf in addition to lat and lon. Ideally
    # this is just time but sometimes there are more.  # From the csv, this is a space-separated list.
    p.dimensions = 'time'

    # # To speed up processing, select which classes you know won't change. For default seals7, this is
    # # the urban classes, the water classes, and the bare land class.
    # p.nonchanging_class_indices = [0, 6, 7]

    # Path to a csv which contains all of the pretrained regressor variables. Can also
    # be 'from_calibration' indicating that this run will actually create the calibration``
    # or it can be from a tile-designated file of location-specific regressor variables.
    p.calibration_parameters_source = 'seals/default_inputs/default_global_coefficients.csv'
    
    # Some data, set to default inputs here, are required to make the model run becayse they determine which classes, which resolutions, ...
    p.key_base_year_lulc_simplified_path = os.path.join('lulc', p.lulc_src_label, p.lulc_simplification_label, 'lulc_' + p.lulc_src_label + '_' + p.lulc_simplification_label + '_' + str(p.key_base_year) + '.tif')
    p.key_base_year_lulc_src_path = os.path.join('lulc', p.lulc_src_label, 'lulc_' + p.lulc_src_label  + '_' + str(p.key_base_year) + '.tif')

    # For convenience, set a synonym for the key_base_year_lulc_simplified_path
    p.base_year_lulc_path = p.key_base_year_lulc_src_path


# Not SURE if this is modelspec, but maybe it would allow custom functions after an input?
def set_derived_attributes(p):
    
    # Resolutions come from the fine and coarse maps
    p.fine_resolution = hb.get_cell_size_from_path(p.base_year_lulc_path)
    p.fine_resolution_arcseconds = hb.pyramid_compatible_resolution_to_arcseconds[p.fine_resolution]
    
    if hb.is_path_gdal_readable(p.coarse_projections_input_path):
        p.coarse_resolution = hb.get_cell_size_from_path(p.coarse_projections_input_path)
        p.coarse_resolution_arcseconds = hb.pyramid_compatible_resolution_to_arcseconds[p.coarse_resolution] 
    else:
        p.coarse_resolution_arcseconds = float(p.coarse_projections_input_path)
        p.coarse_resolution = hb.pyramid_compatible_resolutions[p.coarse_resolution_arcseconds]    
     
    p.fine_resolution_degrees = hb.pyramid_compatible_resolutions[p.fine_resolution_arcseconds]
    p.coarse_resolution_degrees = hb.pyramid_compatible_resolutions[p.coarse_resolution_arcseconds]
    p.fine_resolution = p.fine_resolution_degrees
    p.coarse_resolution = p.coarse_resolution_degrees
    
       
    
    # Set the derived-attributes too whenever the core attributes are set
    p.lulc_correspondence_path = p.get_path(p.lulc_correspondence_path)
    # p.lulc_correspondence_path = hb.get_first_extant_path(p.lulc_correspondence_path, [p.input_dir, p.base_data_dir])
    p.lulc_correspondence_dict = hb.utils.get_reclassification_dict_from_df(p.lulc_correspondence_path, 'src_id', 'dst_id', 'src_label', 'dst_label')
    
    
    p.coarse_correspondence_path = p.get_path(p.coarse_correspondence_path)
    # p.coarse_correspondence_path = hb.get_first_extant_path(p.coarse_correspondence_path, [p.input_dir, p.base_data_dir])
    p.coarse_correspondence_dict = hb.utils.get_reclassification_dict_from_df(p.coarse_correspondence_path, 'src_id', 'dst_id', 'src_label', 'dst_label')

    ## Load the indices and labels from the COARSE correspondence. We need this go get waht calsses are changing.
    if p.coarse_correspondence_dict is not None:

        coarse_dst_ids = p.coarse_correspondence_dict['dst_ids']
        p.coarse_correspondence_class_indices = sorted([int(i) for i in coarse_dst_ids])

        coarse_dst_ids_to_labels = p.coarse_correspondence_dict['dst_ids_to_labels']
        p.coarse_correspondence_class_labels = [str(coarse_dst_ids_to_labels[i]) for i in p.coarse_correspondence_class_indices]


    if p.lulc_correspondence_dict is not None:
        lulc_dst_ids = p.lulc_correspondence_dict['dst_ids']
        p.lulc_correspondence_class_indices = sorted([int(i) for i in lulc_dst_ids])

        lulc_dst_ids_to_labels = p.lulc_correspondence_dict['dst_ids_to_labels']
        p.lulc_correspondence_class_labels = [str(lulc_dst_ids_to_labels[i]) for i in p.lulc_correspondence_class_indices]

    # Define the nonchanging class indices as anything in the lulc simplification classes that is not in the coarse simplification classes
    p.nonchanging_class_indices = [int(i) for i in p.lulc_correspondence_class_indices if i not in p.coarse_correspondence_class_indices] # These are the indices of classes THAT CANNOT EXPAND/CONTRACT


    p.changing_coarse_correspondence_class_indices = [int(i) for i in p.coarse_correspondence_class_indices if i not in p.nonchanging_class_indices] # These are the indices of classes THAT CAN EXPAND/CONTRACT
    p.changing_coarse_correspondence_class_labels = [str(p.coarse_correspondence_dict['dst_ids_to_labels'][i]) for i in p.changing_coarse_correspondence_class_indices if i not in p.nonchanging_class_indices]
    p.changing_lulc_correspondence_class_indices = [int(i) for i in p.lulc_correspondence_class_indices if i not in p.nonchanging_class_indices] # These are the indices of classes THAT CAN EXPAND/CONTRACT
    p.changing_lulc_correspondence_class_labels = [str(p.lulc_correspondence_dict['dst_ids_to_labels'][i]) for i in p.changing_lulc_correspondence_class_indices if i not in p.nonchanging_class_indices]
       
    # From the changing/nonchanging class sets as defined in the lulc correspondence AND the coarse correspondence.
    p.changing_class_indices = p.changing_coarse_correspondence_class_indices + [i for i in p.changing_lulc_correspondence_class_indices if i not in p.changing_coarse_correspondence_class_indices] 
    p.changing_class_labels = p.changing_coarse_correspondence_class_labels + [i for i in p.changing_lulc_correspondence_class_labels if i not in p.changing_coarse_correspondence_class_labels]
    
    p.all_class_indices = p.coarse_correspondence_class_indices + [i for i in p.lulc_correspondence_class_indices if i not in p.coarse_correspondence_class_indices] 
    p.all_class_labels = p.coarse_correspondence_class_labels + [i for i in p.lulc_correspondence_class_labels if i not in p.coarse_correspondence_class_labels]
    p.class_labels = p.all_class_labels
    
    # Check if processing_resolution exists
    if not hasattr(p, 'processing_resolution'):
        p.processing_resolution = 1.0
    
    p.processing_resolution_arcseconds = p.processing_resolution * 3600.0 # MUST BE FLOAT
    
    def parse_model_spec_md(input_md_path):
        out = {}
        return out