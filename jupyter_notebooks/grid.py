import rasterio as rio
from rasterio.mask import mask
import geopandas as gpd

import shapely

import pandas as pd
import json

import os
import matplotlib.pyplot as plt
import numpy as np
from multiprocessing import Pool
from functools import partial
import glob


def find_overlapping_rasters(bbox):
    
    '''
    Returns a list of rasters which will potentially have data at the location of your bounding box.
    '''
    
    overlap = []

    for name, row in index.iterrows():
        
        # Construct a bounding box using the provided clip area
        compare_bbox = shapely.geometry.box(row['minx'], row['miny'], row['maxx'], row['maxy'])
        
        # If the selected bbox is **within** the raster's area
        # (you can change this to '.intersects', but that may include rasters w/ only partial coverage)
        if bbox.within(compare_bbox):
            overlap.append(name)
    return overlap


def mask_and_download(raster, bbox, out_dir, overwrite=False):
    
    '''
    Takes a row of the index .csv and downloads the file, masking it to your bounding box.
    '''
    
    outfile = out_dir + raster.name + '.tif'
    
    # Check if the file already exists
    if os.path.exists(outfile) and not overwrite:
        return
    
    url = raster['fileurl']
    
    # Format url for remote access
    rio_url = 'tar+' + url + '!' + raster.name + '_dem.tif'
    
    print(raster.name)
    
    # Open the raster from the ArcticDEM Server
    try:
        src = rio.open(rio_url)
    except:
        with open(out_dir + raster.name + '_dem_FAILED_OPEN.tif', 'w') as file:
            pass
        return
    
    # Convert bounding box to rasterio desired format
    geo = gpd.GeoDataFrame({'geometry': bbox}, index=[0], crs=src.crs)
    geo = [json.loads(geo.to_json())['features'][0]['geometry']]
    
    # Mask the raster, remove singleton dimensions
    out_img, out_transform = mask(src, shapes=geo, crop=True)
    out_img = np.squeeze(out_img)
    
    # Update the metadata with the new height, width, position, projection
    out_meta = src.meta.copy()
    out_meta.update({'driver':'GTiff',
                     'height': out_img.shape[0],
                     'width': out_img.shape[1],
                     'transform': out_transform,
                     'crs': src.crs
                    })
    
    # Check if the raster contains usable data
    msk = np.ma.masked_equal(out_img, src.nodata)
    if np.all(msk.mask):  # If all of the values are True (nodata)
        print('No data in this raster')
        with open(out_dir + raster.name + '_dem_NO_DATA.tif', 'w') as file:
            pass
        return
    
    # If all good, write the file
    with rio.open(outfile, 'w', **out_meta) as dst:
        dst.write(out_img, 1)
        
    print('Wrote to', outfile)


def extract_all_rasters(grid):
    out_dir = '../data/rasters/grid_rasters/' + str(grid.name) + '/'
    
    if not os.path.exists(out_dir):
        print('Making', out_dir)
        os.makedirs(out_dir)
        
    pool = Pool()
    pool.map(partial(worker, bbox=grid['geometry'], out_dir=out_dir), grid['rasters'])
    

def worker(raster, bbox, out_dir, overwrite=False):
    raster = index.loc[raster]
    
    if os.path.exists(out_dir + raster.name + '_dem.tif') and not overwrite:
        return
    mask_and_download(raster, bbox, out_dir, overwrite)

index = pd.read_pickle('../data/index.pkl')
features = pd.read_pickle('../data/grid_data.pkl')

def main():
    features.apply(extract_all_rasters, axis='columns')
    return

if __name__ == '__main__':
    main()
