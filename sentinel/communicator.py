import re
import os
import glob
import traceback

import fiona
import shutil
import datetime
import pathlib
import numpy as np
import rasterio
from rasterio import mask
import pandas as pd
from rasterio.warp import calculate_default_transform, reproject, Resampling

from sentinel import const


def parse_directories(data, callback):
    base_directory = data["directory"]
    processing_directories = glob.glob(os.path.join(base_directory, "*"))
    directories = []
    for directory in processing_directories:
        directory_name = os.path.basename(directory)
        if re.fullmatch(r"[A-Z0-9]{3}_[A-Z0-9]{6}_\d{8}T\d{6}_N\d{4}_R\d{3}_T[A-Z0-9]{5}_.*", directory_name):
            subdirectories = glob.glob(os.path.join(directory, "GRANULE", "*"))
            for subdirectory in subdirectories:
                subdirectory_name = os.path.basename(subdirectory)
                if re.fullmatch(r"L2A_[A-Z0-9]{6}_[A-Z0-9]{7}_\d{8}T\d{6}", subdirectory_name):
                    directories.append(subdirectory)
        if re.fullmatch(r"L2A_[A-Z0-9]{6}_[A-Z0-9]{7}_\d{8}T\d{6}", directory_name):
            directories.append(directory)
    data["directories"] = directories
    # processing(data, callback)
    process_all(data, callback)


def load_shape(filename):
    with fiona.open(filename, "r") as shapefile:
        crs = shapefile.crs
        shapes = [feature["geometry"] for feature in shapefile]
    return crs, shapes


def process_all(data, callback):
    match_fields = data["match_fields"]
    crs, shapes = load_shape(data["shape"])
    data["shape"] = (crs, shapes)
    pathlib.Path(os.path.join(data["output"], "buffer")).mkdir(parents=True, exist_ok=True)
    for dir_index, directory in enumerate(data["directories"]):
        callback(50 * dir_index // len(data["directories"]), callback_type="percent")
        try:
            process_directory(data, directory, callback)
        except Exception as err:
            callback(f"Unknown error with directory: {directory}, error: {err}",
                     callback_type="error")

    for field_index, field_shape in enumerate(shapes):
        callback(50 + 50 * field_index // len(shapes), callback_type="percent")
        if field_index not in match_fields or match_fields[field_index] not in data["fields"]:
            continue
        try:
            process_field(data, match_fields[field_index], field_shape, callback)
        except Exception as err:
            callback(f"Unknoown error with field {match_fields[field_index]}, error: {err}",
                     callback_type="error")

    shutil.rmtree(os.path.join(data["output"], "buffer"))


def load_r10_scl(path):
    result = None
    with rasterio.open(path) as dataset:
        scale_factor_x = 2
        scale_factor_y = 2

        data = dataset.read(
            out_shape=(
                dataset.count,
                int(dataset.height * scale_factor_y),
                int(dataset.width * scale_factor_x)
            ),
            resampling=Resampling.bilinear
        )

        transform = dataset.transform * dataset.transform.scale(
            (1 / scale_factor_x),
            (1 / scale_factor_y)
        )

        file_crs = dataset.crs
        file_transform = transform
        file_width = dataset.width
        file_height = dataset.height
        result = np.squeeze(data)
    return file_crs, file_transform, file_width, file_height, result


def harmonize(data, coefficient, date):
    offset_coefficients = [
        "B01",
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B07",
        "B08",
        "B8A",
        "B09",
        "B10",
        "B11",
        "B12",
    ]
    offset = 1000
    if coefficient in offset_coefficients and date >= const.harmonize_date:
        data = np.clip(data, offset, 32767) - offset
    return data


def load_coefficient(path, resolution, coefficient, date):
    filename = glob.glob(os.path.join(path, "IMG_DATA", resolution, f"*_{coefficient}_*.jp2"))
    if not filename:
        return None
    filename = filename[0]
    with rasterio.open(filename) as coefficient_file:
        data = coefficient_file.read(1).astype("float32")
        file_crs = coefficient_file.crs
        file_transform = coefficient_file.transform
        file_width = coefficient_file.width
        file_height = coefficient_file.height
    return file_crs, file_transform, file_width, file_height, harmonize(data, coefficient, date)


def process_directory(data, path, callback):
    output = data["output"]
    crs, shapes = data["shape"]
    resolution = data["resolution"]
    date = re.search(r"\d{8}T\d{6}", path).group()
    date = datetime.datetime.strptime(date, "%Y%m%dT%H%M%S")
    date = date.strftime("%Y-%m-%d")
    pathlib.Path(os.path.join(output, "buffer", date)).mkdir(parents=True, exist_ok=True)

    for coefficient in data["coefficients"]:
        try:
            if coefficient == "SCL" and resolution == "R10m":
                scl_filename = glob.glob(os.path.join(path, "IMG_DATA", "R20m", f"*_SCL_*.jp2"))
                if not scl_filename:
                    callback(f"Can't find R20m SCL for R10 SCL, path: {path}",
                             callback_type="error")
                    continue
                file_crs, file_transform, file_width, file_height, coefficient_data = load_r10_scl(scl_filename[0])
            elif coefficient == "NDVI":
                file_crs, file_transform, file_width, file_height, red = load_coefficient(path, resolution, "B04", date)
                file_crs, file_transform, file_width, file_height, nir =\
                    load_coefficient(path, resolution, "B08", date) or load_coefficient(path, resolution, "B8A", date)
                if red is None:
                    callback(f"Can't find B04, path: {path}", callback_type="error")
                    continue
                if nir is None:
                    callback(f"Can't find B08(B8A), path: {path}", callback_type="error")
                    continue
                coefficient_data = (nir - red) / (nir + red)
                del red
                del nir
            elif coefficient == "EVI":
                file_crs, file_transform, file_width, file_height, blue = load_coefficient(path, resolution, "B02", date)
                file_crs, file_transform, file_width, file_height, red = load_coefficient(path, resolution, "B04", date)
                file_crs, file_transform, file_width, file_height, nir = \
                    load_coefficient(path, resolution, "B08", date) or load_coefficient(path, resolution, "B8A", date)
                if blue is None:
                    callback(f"Can't find B02, path: {path}", callback_type="error")
                    continue
                if red is None:
                    callback(f"Can't find B04, path: {path}", callback_type="error")
                    continue
                if nir is None:
                    callback(f"Can't find B08(B8A), path: {path}", callback_type="error")
                    continue
                coefficient_data = 2.5 * (nir - red) / (nir + 6 * red - 7.5 * blue + 1)
                del blue
                del red
                del nir
            else:
                file_crs, file_transform, file_width, file_height, coefficient_data =\
                    load_coefficient(path, resolution, coefficient, date)
                if coefficient_data is None:
                    callback(f"Can't find {coefficient}, path: {path}", callback_type="error")
                    continue

            coefficient_path_no_crs = os.path.join(output, "buffer", date, coefficient + "_no_crs" + ".tiff")
            coefficient_path = os.path.join(output, "buffer", date, coefficient + ".tiff")

            # Записываем каждый коеффициент в файл
            with rasterio.open(coefficient_path_no_crs, "w", driver="GTiff",
                               height=file_height, width=file_width, count=1, dtype="float32", crs=file_crs,
                               transform=file_transform) as coefficient_file:
                coefficient_file.write(coefficient_data, 1)

            # Меняем координатную систему
            with rasterio.open(coefficient_path_no_crs) as coefficient_file:
                transform, width, height = calculate_default_transform(coefficient_file.crs, crs,
                                                                       coefficient_file.width,
                                                                       coefficient_file.height,
                                                                       *coefficient_file.bounds)
                kwargs = coefficient_file.meta.copy()
                kwargs.update({"crs": crs, "transform": transform, "width": width, "height": height})
                with rasterio.open(coefficient_path, "w", **kwargs) as dst:
                    for i in range(1, coefficient_file.count + 1):
                        reproject(
                            source=rasterio.band(coefficient_file, i),
                            destination=rasterio.band(dst, i),
                            src_transform=coefficient_file.transform,
                            src_crs=coefficient_file.crs,
                            dst_transform=transform,
                            dst_crs=crs,
                            resampling=Resampling.nearest)

            # Удаляем файл без координатной системы
            os.remove(coefficient_path_no_crs)
        except Exception as err:
            callback(f"Unknown error with coefficient {coefficient}, error: {err}",
                     callback_type="error")


def process_field(data, field_name, field_shape, callback):
    output = data["output"]

    for coefficient in data["coefficients"]:
        full_data = {}
        dates = []
        for directory in glob.glob(os.path.join(output, "buffer", "*")):
            date = os.path.basename(directory)
            dates.append(date)
            coefficient_path = os.path.join(directory, coefficient + ".tiff")
            field_path = os.path.join(output, "buffer", f"{field_name}_{date}_{coefficient}.tiff")
            try:
                with rasterio.open(coefficient_path) as coefficient_file:
                    try:
                        out_image, out_transform = mask.mask(coefficient_file, [field_shape], crop=True)
                    except ValueError:
                        callback(f"Field {field_name} not found, path: {coefficient_path}",
                                 callback_type="error")
                        continue
                    out_meta = coefficient_file.meta.copy()
                    out_meta.update({"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2],
                                     "transform": out_transform})
                    with rasterio.open(field_path, "w", **out_meta) as dest:
                        dest.write(out_image)
            except rasterio.errors.RasterioIOError as e:
                dates.pop()
                callback(f"Error with field {field_name} date {date}, error: {e}", callback_type="error")
                continue

            with rasterio.open(field_path) as field_file:
                # no_data = field_file.nodata
                no_data = 0.0
                data = []
                val = field_file.read(1, masked=True)
                x_size, y_size = field_file.shape
                for x in range(x_size):
                    for y in range(y_size):
                        if val[x, y] != no_data:
                            data.append((*field_file.xy(x, y), val[x, y]))
            # Удаляем файл обязательно, лишние файлы ломают
            os.remove(os.path.join(output, "buffer", f"{field_name}_{date}_{coefficient}.tiff"))
            for x, y, value in data:
                if (x, y) not in full_data:
                    full_data[(x, y)] = {}
                full_data[(x, y)][date] = value

        rows = []
        for (x, y), data in full_data.items():
            for key in set(dates) - set(data.keys()):
                data[key] = np.nan
            rows.append({"x": x, "y": y, **data})
        df = pd.DataFrame(rows, columns=['x', 'y', *sorted(dates)])
        if len(df.index) == 0:
            continue
        pathlib.Path(os.path.join(output, coefficient)).mkdir(parents=True, exist_ok=True)
        filename = os.path.join(output, coefficient, field_name + ".csv")
        if os.path.isfile(filename):
            df = pd.merge(df, pd.read_csv(filename, sep=const.DELIMITER), on=["x", "y"])
        df = df.loc[:, ~df.columns.duplicated()].copy()  # Удаляет повторяющиеся столбцы
        df.to_csv(filename, index=False, sep=const.DELIMITER)
