import glob
import json
import logging
import os
import pathlib
import re
import shutil
from typing import List, Sequence, Callable, Dict

import rasterio

from processor.communicator import AbstractProcessor
from .const import FORMULAS

logger = logging.getLogger(__name__)


class LandsatProcessor(AbstractProcessor):
    coefficients: List[str]
    directories: List[str]

    def __init__(self, input_path: str, output_path: str, shape_path: str, expected_resolution: int,
                 fields_whitelist: Sequence[str], match_fields: Dict[int, str], coefficients: List[str],
                 callback: Callable):
        super().__init__(input_path, output_path, shape_path, expected_resolution, fields_whitelist, match_fields,
                         callback)
        self.coefficients = coefficients
        self.directories = []

    def parse_directories(self):
        processing_directories = glob.glob(os.path.join(self.input_path, "*"))
        for directory in processing_directories:
            directory_name = os.path.basename(directory)
            if re.fullmatch(r"L.{3}_.{4}_\d{6}_\d{8}_\d{8}_\d{2}_.{2}", directory_name):
                self.directories.append(directory)

    def get_coefficient_path(self, directory, coefficient, *args, **kwargs):
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = args[0]
        filename = metadata["PRODUCT_CONTENTS"].get(f"FILE_NAME_{coefficient}")
        if filename:
            filename = os.path.join(directory, filename)
            if coefficient.startswith("BAND"):
                output_filename = os.path.join(self.buffer_path, os.path.basename(filename))
                if os.path.isfile(output_filename):
                    return output_filename
                with rasterio.open(filename) as dataset:
                    meta = dataset.meta.copy()
                    meta["driver"] = "GTiff"
                    meta["dtype"] = "float32"
                    with rasterio.open(output_filename, 'w', **meta) as output:
                        output.write(dataset.read(1).astype("float32") * 0.0000275 - 0.2, 1)
                return output_filename
            return filename
        if coefficient in FORMULAS:
            return self.get_calculation_coefficient_path(FORMULAS[coefficient], directory, coefficient, metadata)
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

                    dir_name = os.path.basename(directory)
                    with open(os.path.join(directory, dir_name + "_MTL.json")) as metadata_file:
                        metadata = json.load(metadata_file)["LANDSAT_METADATA_FILE"]
                    date = metadata["IMAGE_ATTRIBUTES"]["DATE_ACQUIRED"]

                    for coefficient_index, coefficient in enumerate(self.coefficients):
                        self.callback(100 * (directory_index * len(self.coefficients) + coefficient_index) // (
                                len(self.directories) * len(self.coefficients)), callback_type="percent")
                        path = self.get_coefficient_path(directory, coefficient, metadata)
                        if not path:
                            continue
                        reprojected_path = os.path.join(self.buffer_path, coefficient + "_proc.tif")
                        self.reproject_one(path, reprojected_path)
                        out_path = os.path.join(self.output_path, coefficient)
                        self.process_file(reprojected_path, out_path, date)
                except Exception as e:
                    logger.exception(f"Landsat exception in directory {directory}")
                    self.callback(f"Exception in directory {directory}", callback_type="error")
                    continue
        except Exception:
            logger.exception("LandsatProcessor exception")
            self.callback("Unexpected exception", callback_type="error")
        shutil.rmtree(self.buffer_path, ignore_errors=True)
