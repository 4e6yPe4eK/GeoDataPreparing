import fiona
import shutil
import glob
import numpy as np
import os
import pandas as pd
import pathlib
import rasterio
from rasterio import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling, aligned_target

from drone import const


def load_shape(filename):
    with fiona.open(filename, "r") as shapefile:
        crs = shapefile.crs
        shapes = [feature["geometry"] for feature in shapefile]
    return crs, shapes


def parse_directories(data, callback):
    process_all(data, callback)


def process_all(data, callback):
    crs, shapes = load_shape(data["shape"])
    data["shape"] = (crs, shapes)
    pathlib.Path(os.path.join(data["output"], "buffer")).mkdir(parents=True, exist_ok=True)

    coregister_all(data, callback)
    process_field(data, str(data["index"]), shapes[data["index"]], callback)


def reproj_match(infile, dst_crs, outfile):
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
        dst_transform, dst_width, dst_height = aligned_target(dst_transform, dst_width, dst_height, 0.00054)

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


def coregister_all(data, callback):
    for coefficient in data["coefficients"]:
        full_data = {}
        dates = []
        pathes = glob.glob(os.path.join(data["output"], "buffer", "*"))
        for directory in pathes:
            coefficient_path = os.path.join(directory, coefficient + ".tiff")
            coefficient_path_old = coefficient_path + ".old"
            os.rename(coefficient_path, coefficient_path_old)
            reproj_match(coefficient_path_old, rasterio.crs.CRS.from_epsg(4326), coefficient_path)
            os.remove(coefficient_path_old)


def process_field(data, field_name, field_shape, callback):
    output = data["output"]
    full_data = {}
    dates = []
    files = glob.glob(os.path.join(output, "buffer", "*"))
    for ind, full_filename in enumerate(files):
        callback(50 + int(50 * ind / len(files)))
        filename = os.path.basename(full_filename)
        dates.append(filename)
        filename1, filename2 = filename.rsplit('.', 1)
        cropped_filename = os.path.join(os.path.dirname(full_filename), f"{filename1}_cropped.{filename2}")
        try:
            with rasterio.open(full_filename) as coefficient_file:
                try:
                    out_image, out_transform = mask.mask(coefficient_file, [field_shape], crop=True)
                except ValueError:
                    callback(f"Поле не найдено! Путь: {full_filename}",
                             callback_type="error")
                    continue
                out_meta = coefficient_file.meta.copy()
                out_meta.update({"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2],
                                 "transform": out_transform})
                with rasterio.open(cropped_filename, "w", **out_meta) as dest:
                    dest.write(out_image)
        except rasterio.errors.RasterioIOError as e:
            dates.pop()
            callback(f"Error with field {field_name} {full_filename}, error: {e}", callback_type="error")
            continue

        with rasterio.open(cropped_filename) as field_file:
            # no_data = field_file.nodata
            no_data = 0.0
            data = []
            val = field_file.read(1, masked=True)
            x_size, y_size = field_file.shape
            for x in range(x_size):
                for y in range(y_size):
                    if val[x, y] != no_data:
                        data.append((*field_file.xy(x, y), val[x, y]))
        # Удаляем файл
        os.remove(cropped_filename)
        for x, y, value in data:
            if (x, y) not in full_data:
                full_data[(x, y)] = {}
            full_data[(x, y)][filename] = value
    shutil.rmtree(os.path.join(output, "buffer"))
    rows = []
    for (x, y), data in full_data.items():
        for key in set(dates) - set(data.keys()):
            data[key] = np.nan
        rows.append({"x": x, "y": y, **data})
    df = pd.DataFrame(rows, columns=['x', 'y', *sorted(dates)])
    if len(df.index) == 0:
        return
    filename = os.path.join(output, filename + ".csv")
    if os.path.isfile(filename):
        df = pd.concat([pd.read_csv(filename, sep=const.DELIMITER), df])
    df = df.loc[:, ~df.columns.duplicated()].copy()  # Удаляет повторяющиеся столбцы
    df = df.drop_duplicates(subset=['x', 'y'])  # Удаляет повторяющиеся строки(совпадающие координаты)
    df.to_csv(filename, index=False, sep=const.DELIMITER)
