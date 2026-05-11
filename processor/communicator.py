import ast
import logging
import os
import re
import sqlite3
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
        logger.info(f"{self.__class__.__name__} initializing with input {input_path} and shape {shape_path}")
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
        self.db_path = os.path.join(self.output_path, "result.db")
        self.db_conn = None
        self.db_cur = None
        self._initialize_database()

    def _initialize_database(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS result ("
                           "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                           "coefficient VARCHAR(64), "
                           "field VARCHAR(64), "
                           "date VARCHAR(16), "
                           "longitude REAL, "
                           "latitude REAL,"
                           "value REAL, "
                           "UNIQUE(coefficient, field, date, longitude, latitude)"
                           ")")
            conn.commit()

    def run(self) -> None:
        with sqlite3.connect(self.db_path) as self.db_conn:
            self.db_cur = self.db_conn.cursor()
            self._import_from_csv()
            self._run()
            self._export_to_csv()

    def _run(self) -> None:
        raise NotImplementedError()

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        safe_name = re.sub(r'[\\/*?:"<>|\s]', '_', name)
        safe_name = safe_name.strip('.')
        safe_name = safe_name.strip('_')
        if not safe_name:
            safe_name = "unnamed"
        return safe_name

    def _import_from_csv(self) -> None:
        self.db_cur.execute("SELECT COUNT(*) FROM result")
        if self.db_cur.fetchone()[0] > 0:
            return

        if not os.path.exists(self.output_path):
            return

        for coef_dir in os.listdir(self.output_path):
            coef_path = os.path.join(self.output_path, coef_dir)
            if not os.path.isdir(coef_path):
                continue
            coefficient = coef_dir
            for csv_file in os.listdir(coef_path):
                if not csv_file.endswith('.csv'):
                    continue
                field = csv_file[:-4]
                csv_full_path = os.path.join(coef_path, csv_file)
                try:
                    df = pd.read_csv(csv_full_path, delimiter=DELIMITER)
                    if not {'x', 'y'}.issubset(df.columns):
                        continue

                    date_columns = [col for col in df.columns if col not in ('x', 'y')]
                    df_long = df.melt(id_vars=['x', 'y'], value_vars=date_columns,
                                      var_name='date', value_name='value')
                    df_long.dropna(subset=['value'], inplace=True)
                    if df_long.empty:
                        continue
                    df_long['coefficient'] = coefficient
                    df_long['field'] = field
                    df_long.rename(columns={'x': 'longitude', 'y': 'latitude'}, inplace=True)
                    df_long = df_long[['coefficient', 'field', 'date', 'longitude', 'latitude', 'value']]
                    df_long.to_sql('result', self.db_conn, if_exists='append', index=False,
                                   dtype={'coefficient': 'TEXT', 'field': 'TEXT', 'date': 'TEXT',
                                          'longitude': 'REAL', 'latitude': 'REAL', 'value': 'REAL'})
                except Exception as e:
                    continue
            self.db_conn.commit()

    def _export_to_csv(self) -> None:
        df = pd.read_sql_query(
            "SELECT coefficient, field, date, longitude, latitude, value FROM result",
            self.db_conn
        )

        for (coef, field), group in df.groupby(['coefficient', 'field']):
            coef_safe = self._sanitize_filename(coef)
            field_safe = self._sanitize_filename(field)

            pivot = group.pivot(index=['longitude', 'latitude'], columns='date', values='value')
            pivot.reset_index(inplace=True)
            pivot.rename(columns={'longitude': 'x', 'latitude': 'y'}, inplace=True)

            date_cols = [c for c in pivot.columns if c not in ('x', 'y')]
            date_cols_sorted = sorted(date_cols)
            pivot = pivot[['x', 'y'] + date_cols_sorted]

            coef_dir = os.path.join(self.output_path, coef_safe)
            os.makedirs(coef_dir, exist_ok=True)
            csv_path = os.path.join(coef_dir, f"{field_safe}.csv")
            pivot.to_csv(csv_path, index=False, sep=DELIMITER)

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

    def process_file(self, file_path: str, coefficient: str, date: str) -> None:
        with rasterio.open(file_path) as src:
            for field_index, field_shape in enumerate(self.shapes):
                if not field_shape:
                    continue
                if field_index >= len(self.match_fields) or self.match_fields[field_index] not in self.fields_whitelist:
                    continue
                field_name = self.match_fields[field_index]
                try:
                    try:
                        out_image, out_transform = mask(src, [field_shape], filled=False, crop=True)
                        out_image = np.ma.squeeze(out_image, axis=0)
                    except ValueError:
                        self.callback(f"Field {field_name} is not presented in {file_path}", callback_type="error")
                        continue
                    x_points, y_points = np.where(~out_image.mask)
                    x_coords, y_coords = rasterio.transform.xy(out_transform, x_points, y_points)
                    data = np.array([np.round(x_coords, 6), np.round(y_coords, 6), out_image.compressed()]).T
                    data = [(coefficient, field_name, date, x, y, val) for x, y, val in data]

                    self.db_cur.executemany("INSERT OR IGNORE INTO result "
                                    "(coefficient, field, date, longitude, latitude, value) "
                                    "VALUES (?, ?, ?, ?, ?, ?)", data)
                except Exception as e:
                    logger.exception(f"field_name-{field_name},file-{file_path}")
                    self.callback(f"Error with field {field_name}, file: {file_path}", callback_type="error")
                    continue
            self.db_conn.commit()

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
