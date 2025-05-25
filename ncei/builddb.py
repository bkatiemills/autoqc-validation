import netCDF4, pymongo, json, sys, glob

client = pymongo.MongoClient("mongodb://iquoddb/")
db = client["iquod"]
collection = db["wod"]

for file in glob.glob(sys.argv[1]+"/*"):
    print(file)

    ncfile = netCDF4.Dataset(file, mode="r")

    fields_to_extract = ['time', 'date']
    filetype = file.split('_')[1]

    n_records = len(ncfile.dimensions['casts'])  # or your record dimension
    for i in range(n_records):
        doc = {}
        doc['geolocation'] = {"type": "Point", "coordinates": [float(ncfile.variables['lon'][i]), float(ncfile.variables['lat'][i])]}
        doc['_id'] = int(ncfile.variables['wod_unique_cast'][i])
        doc['filetype'] = filetype
        for field in fields_to_extract:
            var = ncfile.variables[field]
            if var.ndim == 1:
                doc[field] = float(var[i])
            else:
                doc[field] = [float(v) for v in var[i]]

        collection.insert_one(doc)


collection.create_index([("geolocation", "2dsphere")])
