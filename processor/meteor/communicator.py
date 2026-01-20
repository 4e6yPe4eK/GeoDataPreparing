import glob
import logging
import os
import pathlib
import re
import shutil
from typing import List, Dict, Sequence, Callable

from processor.communicator import AbstractProcessor
from .const import FORMULAS

logger = logging.getLogger(__name__)


class MeteorProcessor(AbstractProcessor):
    coefficients: List[str]
    date_coefficient_path: Dict[str, Dict[str, str]]

    def __init__(self, input_path: str, output_path: str, shape_path: str, expected_resolution: int,
                 fields_whitelist: Sequence[str], match_fields: Dict[int, str], coefficients: List[str],
                 callback: Callable):
        super().__init__(input_path, output_path, shape_path, expected_resolution, fields_whitelist, match_fields,
                         callback)
        self.coefficients = coefficients
        self.date_coefficient_path = {}

    def parse_files(self):
        all_directories = glob.glob(os.path.join(self.input_path, "*"))
        for directory in all_directories:
            files = glob.glob(os.path.join(directory, "*.tif"))
            for file in files:
                date = re.findall(r'\d{7}', file)[0][-3:]
                if date not in self.date_coefficient_path:
                    self.date_coefficient_path[date] = {}
                coefficient = "NIR" if "nir" in os.path.basename(file) else "RED"
                self.date_coefficient_path[date][coefficient] = file

    def get_coefficient_path(self, directory, coefficient, *args, **kwargs):
        if 'date' in kwargs:
            date = kwargs['date']
        else:
            date = args[0]
        filename = self.date_coefficient_path.get(date, {}).get(coefficient, None)
        if filename:
            return filename
        if coefficient in FORMULAS:
            return self.get_calculation_coefficient_path(FORMULAS[coefficient], directory, coefficient, date)
        return None

    def run(self):
        self.parse_files()
        for coefficient in self.coefficients:
            path = os.path.join(self.output_path, coefficient)
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        try:
            for date_index, date in enumerate(self.date_coefficient_path):
                try:
                    shutil.rmtree(self.buffer_path, ignore_errors=True)
                    os.makedirs(self.buffer_path)

                    for coefficient_index, coefficient in enumerate(self.coefficients):
                        self.callback(100 * (date_index * len(self.coefficients) + coefficient_index) // (
                                len(self.date_coefficient_path) * len(self.coefficients)), callback_type="percent")
                        path = self.get_coefficient_path("", coefficient, date)
                        if not path:
                            continue
                        reprojected_path = os.path.join(self.buffer_path, coefficient + "_proc.tif")
                        self.reproject_one(path, reprojected_path)
                        out_path = os.path.join(self.output_path, coefficient)
                        self.process_file(reprojected_path, out_path, date)
                except Exception as e:
                    logger.exception(f"Meteor exception in date {date}")
                    self.callback(f"Exception in date {date}", callback_type="error")
                    continue
        except Exception:
            logger.exception("MeteorProcessor exception")
            self.callback("Unexpected exception", callback_type="error")
        shutil.rmtree(self.buffer_path, ignore_errors=True)
