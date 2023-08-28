import re
import os
import glob
import fiona
import datetime
import pathlib
import numpy as np
import const
import rasterio
from rasterio import mask
import logging
import pandas as pd
from rasterio.warp import calculate_default_transform, reproject, Resampling


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
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_AOT_*.jp2"))[0]) as aot_file:
            aot = aot_file.read(1).astype("float32")
            file_crs = aot_file.crs
            file_transform = aot_file.transform
            file_width = aot_file.width
            file_height = aot_file.height
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B02_*.jp2"))[0]) as blue_file:
            blue = blue_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B03_*.jp2"))[0]) as green_file:
            green = green_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B04_*.jp2"))[0]) as red_file:
            red = red_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B05_*.jp2"))[0]) as b05_file:
            b05 = b05_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B06_*.jp2"))[0]) as b06_file:
            b06 = b06_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B07_*.jp2"))[0]) as b07_file:
            b07 = b07_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B8A_*.jp2"))[0]) as nir_file:
            nir = nir_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B11_*.jp2"))[0]) as b11_file:
            b11 = b11_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_B12_*.jp2"))[0]) as b12_file:
            b12 = b12_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_SCL_*.jp2"))[0]) as scl_file:
            scl = scl_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_TCI_*.jp2"))[0]) as tci_file:
            tci = tci_file.read(1).astype("float32")
        with rasterio.open(glob.glob(os.path.join(directories[ind], "IMG_DATA", resolution, "*_WVP_*.jp2"))[0]) as wvp_file:
            wvp = wvp_file.read(1).astype("float32")
        for coefficient_ind, coefficient in enumerate(allowed_coefficients):
            #  TODO: possible index error
            callback((ind * len(allowed_coefficients) + coefficient_ind) * 50 / (len(dates) * len(allowed_coefficients)))
            coefficient_data = np.zeros((1, ))
            if coefficient == "NDVI":
                coefficient_data = (nir - red) / (nir + red)
            elif coefficient == "EVI":
                coefficient_data = 2.5 * (nir - red) / (nir + 6 * red - 7.5 * blue + 1)
            elif coefficient == "AOT":
                coefficient_data = aot
            elif coefficient == "WVP":
                coefficient_data = wvp
            elif coefficient == "SCL":
                coefficient_data = scl
            elif coefficient == "B02":
                coefficient_data = blue
            elif coefficient == "B03":
                coefficient_data = green
            elif coefficient == "B04":
                coefficient_data = red
            elif coefficient == "B05":
                coefficient_data = b05
            elif coefficient == "B06":
                coefficient_data = b06
            elif coefficient == "B07":
                coefficient_data = b07
            elif coefficient == "B8A":
                coefficient_data = nir
            elif coefficient == "B11":
                coefficient_data = b11
            elif coefficient == "B12":
                coefficient_data = b12
            elif coefficient == "TCI":
                coefficient_data = tci
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
                    with rasterio.open(coefficient_path) as coefficient_file:
                        out_image, out_transform = mask.mask(coefficient_file, [field_shape], crop=True)
                        out_meta = coefficient_file.meta.copy()
                        out_meta.update({"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})
                        with rasterio.open(os.path.join(output, match_fields[field_index], date + "_" + coefficient + ".tiff"), "w", **out_meta) as dest:
                            dest.write(out_image)

                    with rasterio.open(os.path.join(output, match_fields[field_index], date + "_" + coefficient + ".tiff")) as coefficient_file:
                        no_data = coefficient_file.nodata
                        data = []
                        val = coefficient_file.read(1)
                        x_size, y_size = coefficient_file.shape
                        for x in range(x_size):
                            for y in range(y_size):
                                if val[x, y] != no_data:
                                    data.append((*coefficient_file.xy(x, y), val[x, y]))
                    # Удаляем файл
                    os.remove(os.path.join(output, match_fields[field_index], date + "_" + coefficient + ".tiff"))
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
            df = pd.DataFrame(rows, columns=['x', 'y', *dates])
            df.to_csv(os.path.join(output, coefficient, match_fields[field_index] + ".csv"), index=False)
