## For prcocessing, around 5GB free space in RAM required
# Import packages to be used for analysis
import numpy as np
import geopandas as gpd
import sqlalchemy as sq
import geoalchemy2 as gq2
import psycopg2 as ps2
import pandas as pd
from secret import engine_int

#Variables
COLUMNS_KEY = ['lot', 'lotplan', 'parcel_typ', 'cover_typ']

#Create postgres engine to the server & db
engine = sq.create_engine(engine_int)


# ---- keydata preparation ----
class key_dataframe_creation:
    """This method extracts one of the base DCDB dataset tables to get key values for DCDB difference.
    This method does not address the datasets derived from MapInfo reissued datasets"""

    def __init__(self, data):
        self.data = data

    # Extract tables necessary for key 'lotplan' extraction to pandas df
    def query_for_key(self):
        query = f"""SELECT lot, lotplan, parcel_typ, cover_typ FROM dcdb.qld_dcdb_{self.data}
        WHERE plan LIKE 'RP%%' OR plan LIKE 'SP%%' OR plan LIKE 'GTP%%' OR plan LIKE 'BUP%%'
        AND parcel_typ = 'Lot Type Parcel' AND cover_typ = 'Base' ORDER BY lotplan"""
        access = pd.read_sql_query(query, engine)
        df = pd.DataFrame(access, columns=COLUMNS_KEY)
        return df[(df['cover_typ'] == 'Base') & (df['parcel_typ'] == 'Lot Type Parcel')]

# ---- geodata preparation -----
class geodata_extraction:
        """This method extact geodataframe from database.
        This method does not address the datasets derived from MapInfo reissued datasets"""
        def __init__(self, data):
            self.data = data

        # Extract tables necessary for final output with geom to geopandas df
        def query_geo(self):
            query_geo = f"""SELECT lot, plan, lotplan, lac, loc, locality, parcel_typ, cover_typ, ca_area_sqm, o_shape
            FROM dcdb.qld_dcdb_{self.data} WHERE plan LIKE 'RP%%' OR plan LIKE 'SP%%' OR plan LIKE 'GTP%%' OR plan LIKE 'BUP%%'
            AND parcel_typ = 'Lot Type Parcel' AND cover_typ = 'Base' ORDER BY lotplan"""
            gdf = gpd.GeoDataFrame.from_postgis(query_geo, engine, geom_col='o_shape')
            # convert sqm to ha / create size categories
            # SQM/10000
            gdf['area_ha'] = gdf['ca_area_sqm']/10000
            # create size category column
            # conditions of size category
            conditions = [
                (gdf['area_ha'] <= 1000),
                (gdf['area_ha'] > 1000) & (gdf['area_ha'] <= 10000),
                (gdf['area_ha'] > 10000)
            ]
            # category names to be added for each condition
            categories = [1,2,3]
            # create a new column and use np.select to assign categories to it using the lists as arguments
            gdf['size_cat'] = np.select(conditions, categories)
            # Delete unnecessary sqm column and reorder columns putting geometry at the end for table consistency with other ones
            gdf.pop('ca_area_sqm')
            mask = gdf.columns.isin(["o_shape"])
            return pd.concat([gdf.loc[:,~mask], gdf.loc[:,mask]], axis=1)


# ---- Processing ----
# Extract the difference between two datasets for key value
def extract_key(new_data, pre_data):
    merged = new_data.merge(pre_data['lotplan'], left_on='lotplan', right_on='lotplan', how='outer',
                                 suffixes=('_n', '_p'), indicator=True)
    return merged[merged['_merge'] == 'left_only']

# compare the key and geodataset to get the result in geodataframe
def newlot_data(key_data, geo_data):
    return geo_data[geo_data['lotplan'].isin(key_data['lotplan'])]

# upload the result to the database creating a new table
def upload_db(newlot_data, period):
    return newlot_data.to_postgis(f"dcdb_change_{period}", engine, index=True,
                                  index_label='id', schema='dcdb_difference', if_exists='replace')


# upload the result to the database creating a new table
def export_shp(newlot_data, period):
    return newlot_data.to_file(f"newlot_{period}.shp")


if __name__ == "__main__":
    y2101 = key_dataframe_creation('21_01')
    y2101_d = y2101.query_for_key()
    y2007 = key_dataframe_creation('20_07')
    y2007_d = y2007.query_for_key()
    y2101_2007_key = extract_key(y2101_d, y2007_d)
    y2101_geo = geodata_extraction('21_01')
    y2101_d = y2101_geo.query_geo()
    result_210107 = newlot_data(y2101_2007_key, y2101_d)
    upload_db(result_210107, '2007_2101')
    print('all process finished!')

