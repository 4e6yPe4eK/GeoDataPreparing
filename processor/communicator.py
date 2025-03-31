import ast
import logging
import os
from typing import List, Set, Sequence, Callable, Dict

import fiona
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from rasterio.warp import aligned_target, calculate_default_transform, reproject, Resampling

from const import DELIMITER


logger = logging.getLogger(__name__)


def load_shape(filename):
    with fiona.open(filename, "r") as shapefile:
        crs = shapefile.crs
        shapes = [feature["geometry"] for feature in shapefile]
    return crs, shapes


class AbstractProcessor:
    input_path: str
    output_path: str
    buffer_path: str
    shapes: list
    crs: rasterio.crs.CRS
    expected_resolution: int
    fields_whitelist: Set[str]
    match_fields: List[str]

    def __init__(self, input_path: str, output_path: str, shape_path: str, expected_resolution: int,
                 fields_whitelist: Sequence[str], match_fields: Dict[int, str], callback: Callable):
        self.input_path = input_path
        self.output_path = output_path
        self.buffer_path = os.path.join(self.output_path, "buffer")
        self.crs, self.shapes = load_shape(shape_path)
        self.expected_resolution = expected_resolution
        self.fields_whitelist = set(fields_whitelist)
        self.match_fields = []
        for i in range(len(self.shapes)):
            self.match_fields.append(match_fields.get(i, "out"))
        self.callback = callback

    def run(self) -> None:
        raise NotImplementedError()

    def reproject_one(self, file_input, file_output):
        with rasterio.open(file_input) as src:
            dst_transform, dst_width, dst_height = calculate_default_transform(
                src.crs,
                self.crs,
                src.width,
                src.height,
                *src.bounds,
            )
            dst_transform, dst_width, dst_height = aligned_target(dst_transform, dst_width, dst_height,
                                                                  self.expected_resolution * 9 / 1000000)
            dst_kwargs = src.meta.copy()
            dst_kwargs.update({"crs": self.crs,
                               "transform": dst_transform,
                               "width": dst_width,
                               "height": dst_height})
            with rasterio.open(file_output, "w", **dst_kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=dst_transform,
                        dst_crs=self.crs,
                        resampling=Resampling.bilinear)

    def process_file(self, file_path: str, output_directory_path: str, date: str) -> None:
        with rasterio.open(file_path) as src:
            for field_index, field_shape in enumerate(self.shapes):
                if field_index >= len(self.match_fields) or self.match_fields[field_index] not in self.fields_whitelist:
                    continue
                field_name = self.match_fields[field_index]
                try:
                    if field_index >= len(self.match_fields) or self.match_fields[
                        field_index] not in self.fields_whitelist:
                        continue
                    try:
                        out_image, out_transform = mask(src, [field_shape], filled=False, crop=True)
                        out_image = np.ma.squeeze(out_image, axis=0)
                    except ValueError:
                        self.callback(f"Field {field_name} is not presented in {file_path}", callback_type="error")
                        continue
                    x_points, y_points = np.where(~out_image.mask)
                    x_coords, y_coords = rasterio.transform.xy(out_transform, x_points, y_points)
                    data = np.array([np.round(x_coords, 6), np.round(y_coords, 6), out_image.compressed()]).T
                    df = pd.DataFrame(data, columns=["x", "y", date])
                    if len(df.index) == 0:
                        continue
                    out_filename = os.path.join(output_directory_path, field_name + ".csv")
                    if os.path.exists(out_filename):
                        try:
                            new_df = pd.merge(df, pd.read_csv(out_filename, sep=DELIMITER), on=['x', 'y'], how='outer',
                                          suffixes=('_x', '_y'))
                            bad_cols = []
                            for col in new_df.columns:
                                if col.endswith("_x"):
                                    bad_cols.append(col[:-2])
                            for base in bad_cols:
                                x_col = f"{base}_x"
                                y_col = f"{base}_y"
                                new_df[base] = new_df[x_col].combine_first(new_df[y_col])
                                new_df.drop(columns=[x_col, y_col], inplace=True)
                            sorted_columns = ['x', 'y'] + sorted([col for col in new_df.columns if col not in ['x', 'y']])
                            df = new_df[sorted_columns]
                        except pd.errors.EmptyDataError:
                            pass
                    df.to_csv(out_filename, index=False, sep=DELIMITER)
                except Exception as e:
                    logger.exception(f"field_name-{field_name},file-{file_path}")
                    self.callback(f"Error with field {field_name}, file: {file_path}", callback_type="error")
                    continue

    def get_coefficient_path(self, directory_path, coefficient, *args, **kwargs):
        raise NotImplementedError()

    def get_calculation_coefficient_path(self, formula, directory_path, coefficient, *args, **kwargs):
        out_filename = os.path.join(self.buffer_path, coefficient + ".tif")
        if os.path.isfile(out_filename):
            return out_filename

        variables = {}

        class VariableExtractor(ast.NodeVisitor):
            def visit_Name(self, node):
                variables[node.id] = None

        try:
            tree = ast.parse(formula, mode='eval')
            extractor = VariableExtractor()
            extractor.visit(tree)
        except SyntaxError:
            return None
        meta = {}
        for var in variables:
            path = self.get_coefficient_path(directory_path, var, *args, **kwargs)
            if not path:
                return None
            with rasterio.open(path) as dataset:
                data = dataset.read(1).astype("float32")
                meta = dataset.meta.copy()
            variables[var] = data
        meta["driver"] = "GTiff"
        meta["dtype"] = "float32"
        with np.errstate(divide='ignore'):
            data = eval(formula, {"__builtins__": {'__import__': __import__}}, variables)

        with rasterio.open(out_filename, 'w', **meta) as output:
            output.write(data, 1)
        return out_filename
