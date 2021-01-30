#!/bin/bash

# npm install -g mapshaper

mapshaper ../data/2018-2012_Election_Data_with_2011_Wards.geojson -clean -simplify 10% keep-shapes -o ../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson
mapshaper ../data/Wisconsin_Assembly_Districts_2012.geojson -clean -simplify 10% keep-shapes -o ../data/simplified/Wisconsin_Assembly_Districts_2012.geojson
mapshaper ../data/Wisconsin_Senate_Districts.geojson -clean -simplify 10% keep-shapes -o ../data/simplified/Wisconsin_Senate_Districts.geojson
mapshaper ../data/Wisconsin_Congressional_Districts_2011.geojson -clean -simplify 10% keep-shapes -o ../data/simplified/Wisconsin_Congressional_Districts_2011.geojson
cp ../data/us-states.geojson ../data/simplified/us-states.geojson
mapshaper ../data/County_Boundaries_24K.geojson -clean -simplify 10% keep-shapes -o ../data/simplified/County_Boundaries_24K.geojson
