import re
import os
import glob
import fiona
import shutil
import datetime
import pathlib
import numpy as np
import rasterio
from rasterio import mask
import logging
import pandas as pd
from rasterio.warp import calculate_default_transform, reproject, Resampling

from sentinel import const


def translate_coefficient_name(coefficient_name):
    """
    Функция, возвращающая удобное название коэффициента
    :param coefficient_name: Оригинальное название коэффициента
    :return: Удобное название коэффициента
    """
    if coefficient_name == "B02":
        return "BLUE"
    if coefficient_name == "B03":
        return "GREEN"
    if coefficient_name == "B04":
        return "RED"
    if coefficient_name == "B08" or coefficient_name == "B8A":
        return "NIR"
    return coefficient_name


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
    processing(data, callback)


def processing(data, callback):
    directories = data["directories"]
    shape = data["shape"]
    output = data["output"]
    resolution = data["resolution"]
    allowed_fields = data["fields"]
    allowed_coefficients = data["coefficients"]
    match_fields = data["match_fields"]
    with fiona.open(shape, "r") as shapefile:
        crs = shapefile.crs
        shapes = [feature["geometry"] for feature in shapefile]
    dates = []
    for directory in directories:
        date = re.search(r"\d{8}T\d{6}", directory).group()
        date = datetime.datetime.strptime(date, "%Y%m%dT%H%M%S")
        dates.append(date.strftime("%Y-%m-%d"))
    pathlib.Path(os.path.join(output, "buffer")).mkdir(parents=True, exist_ok=True)
    for ind, date in enumerate(dates):
        pathlib.Path(os.path.join(output, "buffer", date)).mkdir(parents=True, exist_ok=True)
        all_coefficients = {}

        if resolution == "R10m":  # Individual path for resample SCL
            source_filename = glob.glob(os.path.join(directories[ind], "IMG_DATA", "R20m", f"*_SCL_*.jp2"))
            if source_filename:
                source_filename = source_filename[0]
                with rasterio.open(source_filename) as dataset:
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

                    # scale image transform
                    transform = dataset.transform * dataset.transform.scale(
                        (1 / scale_factor_x),
                        (1 / scale_factor_y)
                    )
                    file_crs = dataset.crs
                    file_transform = transform
                    file_width = dataset.width
                    file_height = dataset.height
                    all_coefficients["SCL"] = np.squeeze(data)
            else:
                print("R20 SCL not found")

        for coefficient_name in (
        const.coefficient_names_r10 if resolution == "R10m" else const.coefficient_names_r20 if resolution == "R20m" else const.coefficient_names_r60):
            filename = glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, f"*_{coefficient_name}_*.jp2"))
            if not filename:
                continue
            filename = filename[0]
            coefficient_name = translate_coefficient_name(coefficient_name)
            with rasterio.open(filename) as file:
                all_coefficients[coefficient_name] = file.read(1).astype("float32")

                # Гармонизация данных
                offset_coefficients = [
                    "B01",
                    "BLUE",
                    "GREEN",
                    "RED",
                    "B05",
                    "B06",
                    "B07",
                    "NIR",
                    "B09",
                    "B10",
                    "B11",
                    "B12",
                ]
                offset = 1000
                if coefficient_name in offset_coefficients and date >= "2022-01-25":
                    all_coefficients[coefficient_name] = np.clip(all_coefficients[coefficient_name], offset, 32767) - offset

                file_crs = file.crs
                file_transform = file.transform
                file_width = file.width
                file_height = file.height
        for coefficient_ind, coefficient in enumerate(allowed_coefficients):
            callback((ind * len(allowed_coefficients) + coefficient_ind) * 50 / (len(dates) * len(allowed_coefficients)))
            coefficient_data = np.zeros((1, ))
            if coefficient == "NDVI":
                coefficient_data = (all_coefficients["NIR"] - all_coefficients["RED"]) / (all_coefficients["NIR"] + all_coefficients["RED"])
            elif coefficient == "EVI":
                coefficient_data = 2.5 * (all_coefficients["NIR"] - all_coefficients["RED"]) / (all_coefficients["NIR"] + 6 * all_coefficients["RED"] - 7.5 * all_coefficients["BLUE"] + 1)
            elif coefficient == "B02":
                coefficient_data = all_coefficients["BLUE"]
            elif coefficient == "B03":
                coefficient_data = all_coefficients["GREEN"]
            elif coefficient == "B04":
                coefficient_data = all_coefficients["RED"]
            elif coefficient == "B8A" or coefficient == "B08":
                coefficient_data = all_coefficients["NIR"]
            elif coefficient in all_coefficients:
                coefficient_data = all_coefficients[coefficient]
            coefficient_path = os.path.join(output, "buffer", date, coefficient + ".tiff")

            # Записываем каждый коеффициент в файл
            with rasterio.open(os.path.join(output, "buffer", date, coefficient + "_no_crs" + ".tiff"), "w", driver="GTiff", height=file_height, width=file_width, count=1, dtype="float32", crs=file_crs, transform=file_transform) as coefficient_file:
                coefficient_file.write(coefficient_data, 1)

            # Меняем координатную систему
            with rasterio.open(os.path.join(output, "buffer", date, coefficient + "_no_crs" + ".tiff")) as coefficient_file:
                transform, width, height = calculate_default_transform(coefficient_file.crs, crs, coefficient_file.width,
                                                                       coefficient_file.height, *coefficient_file.bounds)
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
            os.remove(os.path.join(output, "buffer", date, coefficient + "_no_crs" + ".tiff"))

    # Создаем папки для коэффициентов
    for coefficient in allowed_coefficients:
        pathlib.Path(os.path.join(output, coefficient)).mkdir(parents=True, exist_ok=True)

    for field_index, field_shape in enumerate(shapes):
        callback(50 + field_index * 50 // len(shapes))
        if field_index not in match_fields or match_fields[field_index] not in allowed_fields:
            continue
        for coefficient in allowed_coefficients:
            full_data = {}
            for date in dates:
                try:
                    coefficient_path = os.path.join(output, "buffer", date, coefficient + ".tiff")
                    field_path = os.path.join(output, "buffer", match_fields[field_index] + "_" + date + "_" + coefficient + ".tiff")
                    with rasterio.open(coefficient_path) as coefficient_file:
                        out_image, out_transform = mask.mask(coefficient_file, [field_shape], crop=True)
                        out_meta = coefficient_file.meta.copy()
                        out_meta.update({"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})
                        with rasterio.open(field_path, "w", **out_meta) as dest:
                            dest.write(out_image)

                    with rasterio.open(field_path) as field_file:
                        # TODO: nodata
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
                    os.remove(os.path.join(output, "buffer", match_fields[field_index] + "_" + date + "_" + coefficient + ".tiff"))
                    for x, y, value in data:
                        if (x, y) not in full_data:
                            full_data[(x, y)] = {}
                        full_data[(x, y)][date] = value
                except Exception as err:
                    logging.error(f"field index - {field_index}, field name - {match_fields[field_index]}, Error - {err})")
            rows = []
            for (x, y), data in full_data.items():
                for key in set(dates) - set(data.keys()):
                    data[key] = np.nan
                rows.append({"x": x, "y": y, **data})
            df = pd.DataFrame(rows, columns=['x', 'y', *sorted(dates)])
            if len(df.index) == 0:
                continue
            filename = os.path.join(output, coefficient, match_fields[field_index] + ".csv")
            if os.path.isfile(filename):
                df = pd.concat([pd.read_csv(filename), df])
            df = df.loc[:, ~df.columns.duplicated()].copy()  # Удаляет повторяющиеся столбцы
            df = df.drop_duplicates(subset=['x', 'y'])  # Удаляет повторяющиеся строки(совпадающие координаты)
            df.to_csv(filename, index=False)
    shutil.rmtree(os.path.join(output, "buffer"))
