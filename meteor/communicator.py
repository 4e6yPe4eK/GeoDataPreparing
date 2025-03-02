import glob
import os
import pathlib
import re
import shutil

import fiona
import numpy as np
import pandas as pd
import rasterio
from rasterio import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling, aligned_target

from meteor import const

os.environ["GTIFF_SRS_SOURCE"] = "EPSG"


def load_shape(filename):
    with fiona.open(filename, "r") as shapefile:
        crs = shapefile.crs
        shapes = [feature["geometry"] for feature in shapefile]
    return crs, shapes


def parse_directories(data, callback):
    base_directory = data["directory"]
    all_directories = glob.glob(os.path.join(base_directory, "*"))
    finded_directories = []
    for directory in all_directories:
        finded_directories.append(directory)
    data["directories"] = finded_directories
    process_all(data, callback)


def process_all(data, callback):
    # directories = data["directories"]
    # output = data["output"]
    # resolution = data["resolution"]
    # allowed_fields = data["fields"]
    # allowed_coefficients = data["coefficients"]
    match_fields = data["match_fields"]
    crs, shapes = load_shape(data["shape"])
    data["shape"] = (crs, shapes)
    pathlib.Path(os.path.join(data["output"], "buffer")).mkdir(parents=True, exist_ok=True)
    for dir_ind, directory in enumerate(data["directories"]):
        callback(50 * dir_ind // len(data["directories"]), callback_type="percent")
        try:
            process_directory(data, directory, callback)
        except Exception as err:
            callback(f"Неизвестная ошибка при обработке директории {directory}! Сообщение: {err}",
                     callback_type="error")

    coregister_all(data, callback)

    for field_index, field_shape in enumerate(shapes):
        callback(50 + 50 * field_index // len(shapes), callback_type="percent")
        if field_index not in match_fields or match_fields[field_index] not in data["fields"]:
            continue
        try:
            process_field(data, match_fields[field_index], field_shape, callback)
        except Exception as err:
            callback(f"Неизвестная ошибка при обработке поля {match_fields[field_index]}! Сообщение: {err}",
                     callback_type="error")
    shutil.rmtree(os.path.join(data["output"], "buffer"))


def process_directory(data, path, callback):
    output = data["output"]
    crs, shapes = data["shape"]

    dates = {}

    for file_path in glob.glob(os.path.join(path, '*.tif')):
        filename = os.path.basename(file_path)
        date = re.findall(r'\d{7}', filename)[0][-3:]
        if date not in dates:
            dates[date] = {}
        coefficient = "NIR" if "nir" in filename else "RED"
        dates[date][coefficient] = file_path

    for date_ind, date in enumerate(dates):
        pathlib.Path(os.path.join(output, "buffer", date)).mkdir(parents=True, exist_ok=True)

        # Запись всех коэффициентов в нужном формате
        for coefficient in data["coefficients"]:
            try:
                if coefficient == "NDVI":
                    red_filename = dates[date]["RED"]
                    nir_filename = dates[date]["NIR"]
                    with rasterio.open(red_filename) as red_file:
                        red = red_file.read(1).astype("float32")
                    with rasterio.open(nir_filename) as nir_file:
                        nir = nir_file.read(1).astype("float32")
                        file_crs = nir_file.crs
                        file_transform = nir_file.transform
                        file_width = nir_file.width
                        file_height = nir_file.height
                    nir[nir == 0] = np.nan
                    red[red == 0] = np.nan
                    coefficient_data = (nir - red) / (nir + red)
                    del red
                    del nir
                else:
                    filename = dates[date][coefficient]
                    with rasterio.open(filename) as file:
                        coefficient_data = file.read(1).astype("float32")
                        file_crs = file.crs
                        file_transform = file.transform
                        file_width = file.width
                        file_height = file.height

                coefficient_path = os.path.join(output, "buffer", date, coefficient + ".tiff")

                # Записываем каждый коеффициент в файл
                with rasterio.open(coefficient_path, "w", driver="GTiff",
                                   height=file_height, width=file_width, count=1, dtype="float32", crs=file_crs,
                                   transform=file_transform) as coefficient_file:
                    coefficient_file.write(coefficient_data, 1)
            except Exception as err:
                callback(f"Неизвестная ошибка при обработке коэффициента {coefficient}! Сообщение: {err}",
                         callback_type="error")


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


def coregister_all(data, callback):
    for coefficient in data["coefficients"]:
        pathes = glob.glob(os.path.join(data["output"], "buffer", "*"))
        for directory in pathes:
            coefficient_path = os.path.join(directory, coefficient + ".tiff")
            coefficient_path_old = coefficient_path + ".old"
            os.rename(coefficient_path, coefficient_path_old)
            reproj_match(coefficient_path_old, rasterio.crs.CRS.from_epsg(4326), data["expected_resolution"],
                         coefficient_path)
            os.remove(coefficient_path_old)


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
                        callback(f"Поле {field_name} не найдено! Путь: {coefficient_path}",
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
                val: np.ma.masked_array = field_file.read(1, masked=True)
                x_points, y_points = np.ma.where(val)
                x_coords, y_coords = field_file.xy(x_points, y_points)
                data = np.array([np.round(x_coords, 6), np.round(y_coords, 6), val.compressed()]).T
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
        df['x'] = df['x'].round(6)
        df['y'] = df['y'].round(6)
        if len(df.index) == 0:
            continue
        pathlib.Path(os.path.join(output, coefficient)).mkdir(parents=True, exist_ok=True)
        filename = os.path.join(output, coefficient, field_name + ".csv")
        if os.path.isfile(filename):
            df = pd.merge(df, pd.read_csv(filename, sep=const.DELIMITER), on=['x', 'y'], how='outer',
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
        df = df.loc[:, ~df.columns.duplicated()].copy()  # Удаляет повторяющиеся столбцы
        df.to_csv(filename, index=False, sep=const.DELIMITER)
