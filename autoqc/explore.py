import sqlite3, pymongo, io, pickle, sys
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
length_mismatch = 0
no_match = 0
too_many_matches = 0
qc_match = 0
for row in rows:
    if int(row['year']) != int(sys.argv[1]) or int(row['probe']) != int(sys.argv[2]):
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
            [row['long'] - window, row['lat'] - window]
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

    # count and report whether we got 0, 1, or > 1 matches, and how many match QC exactly
    total += 1
    if len(matches) == 0:
        no_match += 1
    elif len(matches) == 1:
        en_qc = assess_qc(row)
        ncei_qc = matches[0]['temperature_qc_flags']
        if en_qc == ncei_qc:
            qc_match += 1
        else:
            print('------------------')
            print(en_qc)
            print(ncei_qc)
            if len(en_qc) != len(ncei_qc):
                length_mismatch += 1
                print(len(en_qc), len(ncei_qc))
                print(row['lat'], row['long'], row['time'])
                print(matches[0]['geolocation'], matches[0]['time'], matches[0]['date'], matches[0]['filetype'], matches[0]['_id'])
                print(row['raw'])
        unique_match += 1
    elif len(matches) > 1:
        too_many_matches += 1

print(total, no_match, unique_match, length_mismatch, too_many_matches, qc_match)
