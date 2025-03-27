import glob
import logging
import os
import tempfile
from typing import Callable

from processor.communicator import AbstractProcessor

logger = logging.getLogger(__name__)


class DroneProcessor(AbstractProcessor):
    def __init__(self, input_path: str, output_path: str, shape_path: str, expected_resolution: int, shape_index: int,
                 callback: Callable):
        super().__init__(input_path, output_path, shape_path, expected_resolution, [str(shape_index)], [str(i) for i in range(shape_index + 1)], callback)

    def run(self):
        files = glob.glob(os.path.join(self.input_path, "*"))
        for file in files:
            try:
                with tempfile.NamedTemporaryFile() as tmpfile:
                    self.reproject_one(file, tmpfile.name)
                    self.process_file(tmpfile.name, self.output_path, os.path.basename(file))
            except Exception as e:
                logger.exception(f"Error while processing file: {file}")
                self.callback(f"Unexpected exception for file: {file}", callback_type="error")

