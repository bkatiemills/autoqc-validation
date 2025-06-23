import netCDF4, pymongo, json, sys, glob

client = pymongo.MongoClient("mongodb://iquoddb/")
db = client["iquod"]
collection = db["wod"]

for file in glob.glob(sys.argv[1]+"/*"):
    print(file)

    ncfile = netCDF4.Dataset(file, mode="r")

    # include a few fields as is to try matching on
    fields_to_extract = ['time', 'date', 'Access_no']
    filetype = file.split('_')[1]

    n_records = len(ncfile.dimensions['casts'])
    temperature_levels = ncfile.variables['Temperature_row_size']
    iquod_flags = ncfile.variables['Temperature_IQUODflag']
    levelindex = 0
    for i in range(n_records):
        doc = {}
        # going to need to index lon/lat as geojson Point objects
        doc['geolocation'] = {"type": "Point", "coordinates": [float(ncfile.variables['lon'][i]), float(ncfile.variables['lat'][i])]}
        doc['_id'] = int(ncfile.variables['wod_unique_cast'][i])
        doc['filetype'] = filetype
        try:
            nlevels = int(temperature_levels[i])
        except:
            nlevels = 0 # sometimes this number is masked instead of just being 0
        doc['temperature_qc_flags'] = iquod_flags[levelindex:levelindex+nlevels].filled(None).tolist()
        levelindex += nlevels
        for field in fields_to_extract:
            var = ncfile.variables[field]
            if var.ndim == 1:
                doc[field] = float(var[i])
            else:
                doc[field] = [float(v) for v in var[i]]

        collection.insert_one(doc)

collection.create_index([("geolocation", "2dsphere")])



