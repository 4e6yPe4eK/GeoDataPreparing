import glob
import logging
import os
import re
from typing import Optional

import rasterio

from processor.communicator import AbstractProcessor

logger = logging.getLogger(__name__)


def try_extract_date(path: str) -> Optional[str]:
    filename = os.path.basename(path)
    date = re.findall(r"\d{8}", filename)
    if date:
        return date[0]
    return None


class CustomProcessor(AbstractProcessor):
    def run(self):
        if os.path.isfile(self.input_path):
            self._process_file(self.input_path, self.output_path, "CUSTOM")
        if os.path.isdir(self.input_path):
            files = glob.glob(os.path.join(self.input_path, "**", "*.tif"), recursive=True)
            files.extend(glob.glob(os.path.join(self.input_path, "**", "*.tiff"), recursive=True))
            files = [path for path in files if os.path.isfile(path)]
            unknown_count = 0
            for ind, filename in enumerate(files):
                date = try_extract_date(filename)
                if date is None:
                    date = f"CUSTOM_{unknown_count}"
                    unknown_count += 1
                self._process_file(filename, self.output_path, date)
                self.callback(100 * ind // len(files), callback_type="percent")

    def _process_file(self, input_path: str, output_path: str, date: str):
        reprojected_path = os.path.join(output_path, os.path.basename(input_path) + "_proc.tif")
        try:
            with rasterio.open(input_path) as src:
                if src.count != 1:
                    return
            self.reproject_one(input_path, reprojected_path)
            self.process_file(reprojected_path, output_path, date)
        except Exception as e:
            logger.exception("CustomProcessor exception")
            self.callback("Unexpected exception", callback_type="error")
        if os.path.isfile(reprojected_path):
            os.remove(reprojected_path)
