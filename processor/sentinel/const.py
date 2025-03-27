COEFFICIENT_NAMES_R10 = ["NDVI", "EVI", "AOT", "WVP", "SCL", "B02", "B03", "B04", "B08"]
COEFFICIENT_NAMES_R20 = ["NDVI", "EVI", "NDWI-SWIR", "NDWI-Green", "AOT", "WVP", "SCL", "B02", "B03", "B04", "B05", "B06", "B07", "B8A", "B11", "B12"]
COEFFICIENT_NAMES_R60 = ["NDVI", "EVI", "AOT", "WVP", "SCL", "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B8A", "B09", "B11", "B12"]
HARMONIZE_BANDS = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B10", "B11", "B12"]
HARMONIZE_DATE = "2022-01-25"
HARMONIZE_OFFSET = 1000
FORMULAS = {
    "NDVI": "(B08 - B04) / (B08 + B04)",
    "EVI": "2.5 * (B08 - B04) / (B08 + 6 * B04 - 7.5 * B02 + 1)",
    "NDWI-SWIR": "(B08 - B11) / (B08 + B11)",
    "NDWI-Green": "(B03 - B08) / (B03 + B08)",
}
