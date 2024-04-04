import fiona
import shutil
import glob
import json
import numpy as np
import os
import pandas as pd
import pathlib
import rasterio
import re
from rasterio import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling

from landsat import const


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
        directory_name = os.path.basename(directory)
        if re.fullmatch(r"L.{3}_.{4}_\d{6}_\d{8}_\d{8}_\d{2}_.{2}", directory_name):
            finded_directories.append(directory)
    data["directories"] = finded_directories
    process_all(data, callback)


def process_all(data, callback):
    match_fields = data["match_fields"]
    crs, shapes = load_shape(data["shape"])
    data["shape"] = (crs, shapes)
    pathlib.Path(os.path.join(data["output"], "buffer")).mkdir(parents=True, exist_ok=True)
    for directory in data["directories"]:
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


def reproj_match(infile, match, outfile):
    """Reproject a file to match the shape and projection of existing raster.

    Parameters
    ----------
    infile : (string) path to input file to reproject
    match : (string) path to raster with desired shape and projection
    outfile : (string) path to output file tif
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


def coregister_all(data, callback):
    for coefficient in data["coefficients"]:
        full_data = {}
        dates = []
        pathes = glob.glob(os.path.join(data["output"], "buffer", "*"))
        standard = os.path.join(pathes[0], coefficient + ".tiff")
        for directory in pathes[1:]:
            coefficient_path = os.path.join(directory, coefficient + ".tiff")
            coefficient_path_old = coefficient_path + ".old"
            os.rename(coefficient_path, coefficient_path_old)
            reproj_match(coefficient_path_old, standard, coefficient_path)
            os.remove(coefficient_path_old)


def process_directory(data, path, callback):
    output = data["output"]
    crs, shapes = data["shape"]

    dir_name = os.path.basename(path)
    with open(os.path.join(path, dir_name + "_MTL.json")) as metadata_file:
        metadata = json.load(metadata_file)["LANDSAT_METADATA_FILE"]

    date = metadata["IMAGE_ATTRIBUTES"]["DATE_ACQUIRED"]
    pathlib.Path(os.path.join(output, "buffer", date)).mkdir(parents=True, exist_ok=True)

    # Чтение всех коэффициентов
    for coefficient in data["coefficients"]:
        filename = metadata["PRODUCT_CONTENTS"].get(f"FILE_NAME_{coefficient}")

    # Запись всех коэффициентов в нужном формате
    for coefficient in data["coefficients"]:
        try:
            if coefficient == "NDVI":
                red_filename = os.path.join(path, metadata["PRODUCT_CONTENTS"]["FILE_NAME_BAND_4"])
                nir_filename = os.path.join(path, metadata["PRODUCT_CONTENTS"]["FILE_NAME_BAND_5"])
                with rasterio.open(red_filename) as red_file:
                    red = red_file.read(1).astype("float32")
                with rasterio.open(nir_filename) as nir_file:
                    nir = nir_file.read(1).astype("float32")
                    file_crs = nir_file.crs
                    file_transform = nir_file.transform
                    file_width = nir_file.width
                    file_height = nir_file.height
                coefficient_data = (nir - red) / (nir + red)
                del red
                del nir
            elif coefficient == "EVI":
                blue_filename = os.path.join(path, metadata["PRODUCT_CONTENTS"]["FILE_NAME_BAND_2"])
                red_filename = os.path.join(path, metadata["PRODUCT_CONTENTS"]["FILE_NAME_BAND_4"])
                nir_filename = os.path.join(path, metadata["PRODUCT_CONTENTS"]["FILE_NAME_BAND_5"])
                with rasterio.open(blue_filename) as blue_file:
                    blue = blue_file.read(1).astype("float32")
                with rasterio.open(red_filename) as red_file:
                    red = red_file.read(1).astype("float32")
                with rasterio.open(nir_filename) as nir_file:
                    nir = nir_file.read(1).astype("float32")
                    file_crs = nir_file.crs
                    file_transform = nir_file.transform
                    file_width = nir_file.width
                    file_height = nir_file.height
                coefficient_data = 2.5 * (nir - red) / (nir + 6 * red - 7.5 * blue + 1)
                del blue
                del red
                del nir
            else:
                filename = os.path.join(path, metadata["PRODUCT_CONTENTS"][f"FILE_NAME_{coefficient}"])
                with rasterio.open(filename) as file:
                    coefficient_data = file.read(1).astype("float32")
                    file_crs = file.crs
                    file_transform = file.transform
                    file_width = file.width
                    file_height = file.height

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
            callback(f"Неизвестная ошибка при обработке коэффициента {coefficient}! Сообщение: {err}",
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
            # Удаляем файл
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
            df = pd.concat([pd.read_csv(filename, sep=const.DELIMITER), df])
        df = df.loc[:, ~df.columns.duplicated()].copy()  # Удаляет повторяющиеся столбцы
        df = df.drop_duplicates(subset=['x', 'y'])  # Удаляет повторяющиеся строки(совпадающие координаты)
        df.to_csv(filename, index=False, sep=const.DELIMITER)
