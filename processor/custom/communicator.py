import os
import logging
from typing import List, Sequence, Callable

import rasterio

from processor.communicator import AbstractProcessor

logger = logging.getLogger(__name__)


class CustomProcessor(AbstractProcessor):
    def run(self):
        reprojected_path = os.path.join(self.output_path, os.path.basename(self.input_path) + "_proc.tif")
        try:
            with rasterio.open(self.input_path) as src:
                if src.count != 1:
                    return
            self.reproject_one(self.input_path, reprojected_path)
            self.process_file(reprojected_path, self.output_path, "CUSTOM")
        except Exception as e:
            logger.exception("CustomProcessor exception")
            self.callback("Unexpected exception", callback_type="error")
        if os.path.isfile(reprojected_path):
            os.remove(reprojected_path)



