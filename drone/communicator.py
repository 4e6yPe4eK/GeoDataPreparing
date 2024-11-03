import fiona
import shutil
import glob
import numpy as np
import os
import pandas as pd
import pathlib
import rasterio
from rasterio import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling

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


def reproj_match_rescale(infile, match, outfile, factor=1):
    """Reproject a file to match the shape and projection of existing raster.

    Parameters
    ----------
    infile : (string) path to input file to reproject
    match : (string) path to raster with desired shape and projection
    outfile : (string) path to output file tif
    factor: (float) scaling factor
    """
    # open input
    with rasterio.open(infile) as src:
        src_transform = src.transform

        # open input to match
        with rasterio.open(match) as match:
            dst_crs = match.crs

            # calculate the output transform matrix
            dst_transform, dst_width, dst_height = calculate_default_transform(
                src.crs,  # input CRS
                dst_crs,  # output CRS
                match.width,  # input width
                match.height,  # input height
                *match.bounds,  # unpacks input outer boundaries (left, bottom, right, top)
            )

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
                    resampling=Resampling.nearest)
    rescale(outfile, factor)


def rescale(path, factor):
    with rasterio.open(path) as dataset:
        data = dataset.read(
            out_shape=(
                dataset.count,
                int(dataset.height * factor),
                int(dataset.width * factor)
            ),
            resampling=Resampling.bilinear
        )

        transform = dataset.transform * dataset.transform.scale(
            (1 / factor),
            (1 / factor)
        )

        file_crs = dataset.crs
        file_transform = transform
        file_width = int(dataset.width * factor)
        file_height = int(dataset.height * factor)
        result = np.squeeze(data)
        kwargs = dataset.meta.copy()
    kwargs.update({
        "crs": file_crs,
        "transform": file_transform,
        "width": file_width,
        "height": file_height
    })
    with rasterio.open(path, "w", **kwargs) as write_file:
        write_file.write(result, 1)


def coregister_all(data, callback):
    pathes = glob.glob(os.path.join(data["directory"], "*"))
    standard = pathes[0]
    for ind, filename in enumerate(pathes):
        callback(int(50 * ind / len(pathes)))
        out_filename = os.path.join(data["output"], "buffer", os.path.basename(filename))
        reproj_match_rescale(filename, standard, out_filename, data["scale"])


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
