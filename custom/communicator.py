import os
import pathlib

import fiona
import rasterio
from rasterio.warp import aligned_target, calculate_default_transform, reproject, Resampling
from rasterio.mask import mask
import numpy as np
import pandas as pd

from custom import const


def load_shape(filename):
    with fiona.open(filename, "r") as shapefile:
        crs = shapefile.crs
        shapes = [feature["geometry"] for feature in shapefile]
    return crs, shapes


def run(data, callback):
    path = data["path"]
    buf_path = os.path.join(data["output"], os.path.basename(path))
    with rasterio.open(path) as src:
        if src.count != 1:
            callback("Многослойные снимки не поддерживаются!", callback_type="error")
            return
    try:
        reproj_match(path, rasterio.crs.CRS.from_epsg(4326), data["expected_resolution"], buf_path)
    except Exception as e:
        callback(f"Ошибка во время корегистрации: {e}", callback_type="error")
    try:
        process_file(data, buf_path, callback)
    except Exception as e:
        import traceback
        traceback.print_exception(e)
        callback(f"Ошибка во время обработки: {e}", callback_type="error")
    if os.path.exists(buf_path):
        os.remove(buf_path)


def reproj_match(infile, dst_crs, expected_resolution, outfile):
    # open input
    with rasterio.open(infile) as src:
        src_transform = src.transform

        # calculate the output transform matrix
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src.crs,  # input CRS
            dst_crs,  # output CRS
            src.width,  # input width
            src.height,  # input height
            *src.bounds,  # unpacks input outer boundaries (left, bottom, right, top)
        )
        dst_transform, dst_width, dst_height = aligned_target(dst_transform, dst_width, dst_height,
                                                              expected_resolution * 9 / 1000000)

        # set properties for output
        dst_kwargs = src.meta.copy()
        dst_kwargs.update({"crs": dst_crs,
                           "transform": dst_transform,
                           "width": dst_width,
                           "height": dst_height,
                           "nodata": 0})
        # open output
        with rasterio.open(outfile, "w", **dst_kwargs) as dst:
            # iterate through bands and write using reproject function
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear)


def process_file(context_data, filename, callback):
    crs, shapes = load_shape(context_data["shape"])
    match_fields = context_data["match_fields"]
    with rasterio.open(filename) as src:
        for field_index, field_shape in enumerate(shapes):
            callback(100 * field_index // len(shapes))
            if field_index not in match_fields or match_fields[field_index] not in context_data["fields"]:
                continue
            try:
                out_image, _ = mask(src, [field_shape], filled=False, crop=True)
                out_image = np.ma.squeeze(out_image)
            except ValueError:
                continue
            x_points, y_points = np.ma.where(out_image)
            x_coords, y_coords = src.xy(x_points, y_points)
            data = np.array([np.round(x_coords, 6), np.round(y_coords, 6), out_image.compressed()]).T
            df = pd.DataFrame(data, columns=["x", "y", "custom"])
            if len(df.index) == 0:
                continue
            out_filename = os.path.join(context_data["output"], match_fields[field_index] + ".csv")
            if os.path.isfile(out_filename):
                df = pd.merge(df, pd.read_csv(out_filename, sep=const.DELIMITER), on=['x', 'y'], how='outer',
                              suffixes=('_x', '_y'))
                bad_cols = []
                for col in df.columns:
                    if col.endswith("_x"):
                        bad_cols.append(col[:-2])
                for base in bad_cols:
                    x_col = f"{base}_x"
                    y_col = f"{base}_y"
                    df[base] = df[x_col].combine_first(df[y_col])
                    df.drop(columns=[x_col, y_col], inplace=True)
            df.to_csv(out_filename, index=False, sep=const.DELIMITER)
