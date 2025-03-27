import datetime
import glob
import logging
import os
import pathlib
import re
import shutil
from typing import List, Sequence, Callable, Literal

import numpy as np
import rasterio

from processor.communicator import AbstractProcessor
from .const import HARMONIZE_BANDS, HARMONIZE_DATE, HARMONIZE_OFFSET, FORMULAS

logger = logging.getLogger(__name__)


class SentinelProcessor(AbstractProcessor):
    source_resolution: Literal["R10m", "R20m", "R60m"]
    coefficients: List[str]
    directories: List[str]

    def __init__(self, input_path: str, output_path: str, shape_path: str, expected_resolution: int,
                 fields_whitelist: Sequence[str], match_fields: List[str],
                 source_resolution: Literal["R10m", "R20m", "R60m"], coefficients: List[str],
                 callback: Callable):
        super().__init__(input_path, output_path, shape_path, expected_resolution, fields_whitelist, match_fields,
                         callback)
        self.source_resolution = source_resolution
        self.coefficients = coefficients
        self.directories = []

    def parse_directories(self):
        processing_directories = glob.glob(os.path.join(self.input_path, "*"))
        for directory in processing_directories:
            directory_name = os.path.basename(directory)
            if re.fullmatch(r"[A-Z0-9]{3}_[A-Z0-9]{6}_\d{8}T\d{6}_N\d{4}_R\d{3}_T[A-Z0-9]{5}_.*", directory_name):
                subdirectories = glob.glob(os.path.join(directory, "GRANULE", "*"))
                for subdirectory in subdirectories:
                    subdirectory_name = os.path.basename(subdirectory)
                    if re.fullmatch(r"L2A_[A-Z0-9]{6}_[A-Z0-9]{7}_\d{8}T\d{6}", subdirectory_name):
                        self.directories.append(subdirectory)
            if re.fullmatch(r"L2A_[A-Z0-9]{6}_[A-Z0-9]{7}_\d{8}T\d{6}", directory_name):
                self.directories.append(directory)

    def get_coefficient_path(self, directory_path, coefficient, *args, **kwargs):
        if 'date' in kwargs:
            date = kwargs['date']
        else:
            date = args[0]
        filename = glob.glob(os.path.join(directory_path, "IMG_DATA", self.source_resolution, f"*_{coefficient}_*.jp2"))
        if filename:
            filename = filename[0]
            if coefficient in HARMONIZE_BANDS and date >= HARMONIZE_DATE:
                output_filename = os.path.join(self.buffer_path, os.path.basename(filename))
                if os.path.isfile(output_filename):
                    return output_filename
                with rasterio.open(filename) as dataset:
                    with rasterio.open(output_filename, 'w', **dataset.meta) as output:
                        output.write(np.clip(dataset.read(1), HARMONIZE_OFFSET, 32767) - HARMONIZE_OFFSET, 1)
                return output_filename
            return filename
        if coefficient == "B08":
            return self.get_coefficient_path(directory_path, "B8A", date)
        if coefficient == "SCL" and self.source_resolution == "R10m":
            scl_filename = glob.glob(os.path.join(directory_path, "IMG_DATA", "R20m", f"*_SCL_*.jp2"))
            if scl_filename:
                return scl_filename[0]
            return None
        if coefficient in FORMULAS:
            formula = FORMULAS[coefficient]
            return self.get_calculation_coefficient_path(formula, directory_path, coefficient, )
        return None

    def run(self):
        self.parse_directories()
        for coefficient in self.coefficients:
            path = os.path.join(self.output_path, coefficient)
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        try:
            for directory_index, directory in enumerate(self.directories):
                try:
                    shutil.rmtree(self.buffer_path, ignore_errors=True)
                    os.makedirs(self.buffer_path)
                    date = re.search(r"\d{8}T\d{6}", directory).group()
                    date = datetime.datetime.strptime(date, "%Y%m%dT%H%M%S")
                    date = date.strftime("%Y-%m-%d")
                    for coefficient_index, coefficient in enumerate(self.coefficients):
                        self.callback(100 * (directory_index * len(self.coefficients) + coefficient_index) // (
                                    len(self.directories) * len(self.coefficients)), callback_type="percent")
                        path = self.get_coefficient_path(directory, coefficient, date)
                        if not path:
                            continue
                        reprojected_path = os.path.join(self.buffer_path, coefficient + "_proc.tif")
                        self.reproject_one(path, reprojected_path)
                        out_path = os.path.join(self.output_path, coefficient)
                        self.process_file(reprojected_path, out_path, date)
                except Exception as e:
                    logger.exception("Sentinel directory exception")
                    self.callback(f"Exception in directory {directory}", callback_type="error")
                    continue
        except Exception:
            logger.exception("SentinelProcessor exception")
            self.callback("Unexpected exception", callback_type="error")
        shutil.rmtree(self.buffer_path, ignore_errors=True)
