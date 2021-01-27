import json

election_filename = "../data/2018-2012_Election_Data_with_2011_Wards.geojson"

with open(election_filename, "r") as election_file:
    election_data = json.load(election_file)

wsa18_data = {
    "type": "FeatureCollection",
    "features": []
}


def rel(tot: int, rep: int, dem: int) -> float:
    if rep > dem:
        return -(((rep / tot) - 0.5) / 0.5)
    elif dem > rep:
        return ((dem / tot) - 0.5) / 0.5
    else:
        return 0.


for feature in election_data["features"]:
    wsa18_data["features"].append({
        "type": "Feature",
        "properties": {
            "WSAREL18": rel(int(feature["properties"]["WSATOT18"]),
                            int(feature["properties"]["WSAREP18"]),
                            int(feature["properties"]["WSADEM18"]))
        },
        "geometry": feature["geometry"],
    })

wsa18_filename = "../data/wsa18_municipal_wards.json"

with open(wsa18_filename, "w") as wsa18_file:
    json.dump(wsa18_data, wsa18_file)
