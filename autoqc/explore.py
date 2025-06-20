import sqlite3, pymongo, io, pickle
import numpy as np

# sql
conn = sqlite3.connect("iquod.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# mongo
client = pymongo.MongoClient("mongodb://iquoddb/")
db = client["iquod"]
collection = db["wod"]

cur.execute("SELECT * FROM quota WHERE year==1991")
rows = cur.fetchall()

#column_names = [description[0] for description in cur.description]
#print(column_names)
#print(pickle.loads(rows[0]['csiro_wire_break']))
#print(pickle.loads(rows[0]['argo_gradient_test']))

#['raw', 'truth', 'uid', 'year', 'month', 'day', 'time', 'lat', 'long', 'country', 'cruise', 'ocruise', 'probe', 'training', 'flagged', 
#'aoml_climatology_test', 'aoml_constant', 'aoml_gradient', 'aoml_gross', 'aoml_spike', 'argo_global_range_check', 'argo_gradient_test', 'argo_impossible_date_test', 
#'argo_impossible_location_test', 'argo_pressure_increasing_test', 'argo_regional_range_test', 'argo_spike_test', 'csiro_constant_bottom', 'csiro_depth', 'csiro_long_gradient', 
#'csiro_short_gradient', 'csiro_surface_spikes', 'csiro_wire_break', 'cotede_argo_density_inversion', 'cotede_gtspp_woa_normbias', 'cotede_gtspp_global_range', 'cotede_gtspp_gradient', 
#'cotede_gtspp_profile_envelop', 'cotede_gtspp_spike_check', 'cotede_morello2014', 'cotede_woa_normbias', 'cotede_anomaly_detection', 'cotede_digit_roll_over', 'cotede_fuzzy_logic', 
#'cotede_gradient', 'cotede_location_at_sea_test', 'cotede_rate_of_change', 'cotede_spike', 'cotede_tukey53h', 'cotede_tukey53h_norm', 'en_background_available_check', 'en_background_check', 
#'en_constant_value_check', 'en_increasing_depth_check', 'en_range_check', 'en_spike_and_step_check', 'en_spike_and_step_suspect', 'en_stability_check', 'en_std_lev_bkg_and_buddy_check', 
#'en_track_check', 'icdc_aqc_01_level_order', 'icdc_aqc_02_crude_range', 'icdc_aqc_04_max_obs_depth', 'icdc_aqc_05_stuck_value', 'icdc_aqc_06_n_temperature_extrema', 'icdc_aqc_07_spike_check', 
#'icdc_aqc_08_gradient_check', 'icdc_aqc_09_local_climatology_check', 'icdc_aqc_10_local_climatology_check', 'iquod_bottom', 'iquod_gross_range_check', 'wod_gradient_check', 'wod_range_check', 
#'loose_location_at_sea', 'minmax']

# from www.frontiersin.org/journals/marine-science/articles/10.3389/fmars.2022.1075510/full
# 'csiro_surface_spikes' == 'CS XBT surf. temp'?
def propagate_max(lst):
    result = []
    current_max = 0
    for val in lst:
        current_max = max(current_max, val)
        result.append(current_max)
    return result

def assess_qc(row):
    LFPR_tests = ['argo_impossible_date_test', 'argo_impossible_location_test', 'loose_location_at_sea', 'icdc_aqc_01_level_order', 'iquod_gross_range_check', 'wod_range_check', 'icdc_aqc_02_crude_range', 'en_background_check', 'en_std_lev_bkg_and_buddy_check', 'en_increasing_depth_check', 'icdc_aqc_05_stuck_value', 'en_spike_and_step_check', 'csiro_long_gradient', 'en_stability_check']
    COMP_tests = ['argo_impossible_date_test', 'argo_impossible_location_test', 'en_background_available_check', 'icdc_aqc_01_level_order', 'csiro_surface_spikes', 'iquod_gross_range_check', 'wod_range_check', 'aoml_climatology_test', 'cotede_gtspp_woa_normbias', 'en_increasing_depth_check', 'en_constant_value_check', 'en_spike_and_step_check', 'csiro_long_gradient', 'icdc_aqc_08_gradient_check', 'en_stability_check']
    HTPR_tests = ['argo_impossible_date_test', 'argo_impossible_location_test', 'iquod_bottom', 'en_background_available_check', 'icdc_aqc_01_level_order', 'csiro_surface_spikes', 'csiro_wire_break', 'iquod_gross_range_check', 'argo_global_range_check', 'en_range_check', 'icdc_aqc_09_local_climatology_check', 'icdc_aqc_10_local_climatology_check', 'cotede_gtspp_woa_normbias', 'aoml_climatology_test', 'en_std_lev_bkg_and_buddy_check', 'en_constant_value_check', 'csiro_constant_bottom', 'aoml_constant', 'icdc_aqc_06_n_temperature_extrema', 'argo_spike_test', 'cotede_tukey53h', 'icdc_aqc_07_spike_check', 'aoml_spike', 'en_spike_and_step_suspect', 'csiro_long_gradient', 'aoml_gradient', 'icdc_aqc_08_gradient_check', 'csiro_short_gradient', 'cotede_anomaly_detection']

    LFPR_results = [ pickle.loads(row[test]).tolist() for test in LFPR_tests ]
    LFPR_results = list(map(list, zip(*LFPR_results))) # transpose
    LFPR_results = [any(x) for x in LFPR_results]

    COMP_results = [ pickle.loads(row[test]).tolist() for test in COMP_tests ]
    COMP_results = list(map(list, zip(*COMP_results))) # transpose
    COMP_results = [any(x) for x in COMP_results]

    HTPR_results = [ pickle.loads(row[test]).tolist() for test in HTPR_tests ]
    HTPR_results = list(map(list, zip(*HTPR_results))) # transpose
    HTPR_results = [any(x) for x in HTPR_results]

    ncei_result = []
    for i in range(len(LFPR_results)):
        qc = 0
        if LFPR_results[i]:
            qc = 4
        elif COMP_results[i]:
            qc = 3
        elif HTPR_results[i]:
            qc = 2
        else:
            qc = 1
        ncei_result.append(qc)

    #return ncei_result
    return propagate_max(ncei_result)

total = 0
unique_match = 0
no_match = 0
too_many_matches = 0
qc_match = 0
for row in rows:
    if row['year'] != 1991:
        continue
    datenum = row['year']*10000+row['month']*100+row['day']
    window = 0.01
    polygon = {
        "type": "Polygon",
        "coordinates": [[
            [row['long'] - window, row['lat'] - window],
            [row['long'] - window, row['lat'] + window],
            [row['long'] + window, row['lat'] + window],
            [row['long'] + window, row['lat'] - window],
            [row['long'] - window, row['lat'] - window]  # close polygon
        ]]
    }

    query = {
        "geolocation": {
            "$geoWithin": {
                "$geometry": polygon
            }
        },
        "date": datenum
    }

    matches = list(collection.find(query))
    try:
        matches = [x for x in matches if abs(x['time']%1 - row['time']/24)<0.001]
    except:
        pass

    total += 1
    if len(matches) == 0:
        no_match += 1
        #print(datenum, row['time'], row['long'], row['lat'], row['probe'])
    if len(matches) == 1:
        en_qc = assess_qc(row)
        ncei_qc = matches[0]['temperature_qc_flags']
        print('-------------')
        print(en_qc)
        print(ncei_qc)
        if en_qc == ncei_qc:
            qc_match += 1
        if len(en_qc) == len(ncei_qc):
            unique_match += 1

    if len(matches) > 1:
        too_many_matches += 1
        #print(datenum, row['time'], row['long'], row['lat'], row['probe'])
        #print(matches)

print(total, no_match, unique_match, too_many_matches, qc_match)
