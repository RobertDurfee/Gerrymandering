#!/bin/bash

# npm install -g mapshaper

mapshaper ../data/2018-2012_Election_Data_with_2011_Wards.geojson -simplify 10% keep-shapes -o ../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson
mapshaper ../data/Wisconsin_Assembly_Districts_2012.geojson -simplify 10% keep-shapes -o ../data/simplified/Wisconsin_Assembly_Districts_2012.geojson
mapshaper ../data/Wisconsin_Senate_Districts.geojson -simplify 10% keep-shapes -o ../data/simplified/Wisconsin_Senate_Districts.geojson
mapshaper ../data/Wisconsin_Congressional_Districts_2011.geojson -simplify 10% keep-shapes -o ../data/simplified/Wisconsin_Congressional_Districts_2011.geojson
mapshaper ../data/Wisconsin_State_Boundary_24K.geojson -simplify 10% keep-shapes -o ../data/simplified/Wisconsin_State_Boundary_24K.geojson
mapshaper ../data/County_Boundaries_24K.geojson -simplify 10% keep-shapes -o ../data/simplified/County_Boundaries_24K.geojson
