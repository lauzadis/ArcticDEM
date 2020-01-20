import geopandas as gpd
import rasterio as rio
import os
import pandas as pd
import numpy as np
import json
from rasterio.mask import mask
from pandarallel import pandarallel
from functools import partial
from multiprocessing import Pool


def find_grid_intersections(raster):
    intersection = []
    for _, grid in grids.iterrows():
        if grid['geometry'].intersects(raster['geometry']):
            intersection.append(grid)
    return intersection


def mask_grids(raster, overwrite=False):
    rio_url = 'tar+' + raster['fileurl'] + '!' + raster.name + '_dem.tif'
    try:
        src = rio.open(rio_url)
    except:
        with open('./data/grids/' + raster.name + '.txt', 'w') as dst:
            pass
        return


    for grid in find_grid_intersections(raster):
        out_dir = './data/grids/' + str(grid.name) + '/'
        if not os.path.exists(out_dir):
#             print('Making', out_dir)
            os.makedirs(out_dir)
    
        geo = gpd.GeoDataFrame({'geometry': grid['geometry']}, index=[0], crs=src.crs)
        geo = [json.loads(geo.to_json())['features'][0]['geometry']]
        
        out_img, out_transform = mask(src, shapes=geo, crop=True)
        out_img = np.squeeze(out_img)

        out_meta = src.meta.copy()
        out_meta.update({'driver':'GTiff',
                         'height': out_img.shape[0],
                         'width': out_img.shape[1],
                         'transform': out_transform,
                         'crs': src.crs
                        })
        
        
        msk = np.ma.masked_equal(out_img, src.nodata)
        if np.all(msk.mask):  # If all of the values are True (nodata)
            outfile = out_dir + raster.name + '_NODATA_dem.tif'
        else:
            #out_img += raster['dz']
            outfile = out_dir + raster.name + '_dem.tif'

        with rio.open(outfile, 'w', **out_meta) as dst:
            dst.write(out_img, 1)
    return


index = pd.read_pickle('./data/index.pkl')
grids = gpd.read_file('../../data/grid_shapefile/1km_filtered/filtered.shp')
grids['id'] = grids['id'].astype(int)
grids = grids.set_index('id', drop=True)


def main():
    pandarallel.initialize(progress_bar=True)
    index.apply(mask_grids, axis='columns')

if __name__ == '__main__':
    main()
