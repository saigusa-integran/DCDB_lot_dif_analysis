## For prcocessing, around 5GB free space in RAM required
# Import packages to be used for analysis
import re
import numpy as np
import geopandas as gpd
import sqlalchemy as sq
import geoalchemy2 as gq2
import psycopg2 as ps2
import pandas as pd
from secret import engine_int

#Variables
COLUMNS_NEWKEY = ['lot', 'lotplan', 'parcel_typ', 'cover_typ']
COLUMNS_OLDKEY = ['lot_num', 'lot_plan', 'parcel_typ', 'coverage_t']
#Create postgres engine to the server & db
engine = sq.create_engine(engine_int)


# ---- keydata preparation ----
class KeyDFCreation:
    """This method extracts one of the base DCDB dataset tables to get key values for DCDB difference.
    This method does not address the datasets derived from MapInfo reissued datasets"""
    def __init__(self, data):
        self.data = data

    # Extract tables necessary for key 'lotplan' extraction to pandas df
    ##Check which one of self.data or data is correct to place as a variable in class
    def query_for_key(self):
        if self.data[0:1] == '2':
            query = f"""SELECT lot, lotplan, parcel_typ, cover_typ FROM dcdb.qld_dcdb_{self.data}
            WHERE plan LIKE 'RP%%' OR plan LIKE 'SP%%' OR plan LIKE 'GTP%%' OR plan LIKE 'BUP%%'
            AND parcel_typ = 'Lot Type Parcel' AND cover_typ = 'Base' ORDER BY lotplan"""
            access = pd.read_sql_query(query, engine)
            df = pd.DataFrame(access, columns=COLUMNS_NEWKEY)
            return df[(df['cover_typ'] == 'Base') & (df['parcel_typ'] == 'Lot Type Parcel')]
        else:
            query = f"""SELECT lot_num, lot_plan, parcel_typ, coverage_t FROM dcdb.dcdb_{self.data}
            WHERE plan_num LIKE 'RP%%' OR plan_num LIKE 'SP%%' OR plan_num LIKE 'GTP%%' OR plan_num LIKE 'BUP%%'
            AND parcel_typ = 'L' AND coverage_t = 'B' ORDER BY lot_plan"""
            access = pd.read_sql_query(query, engine)
            df = pd.DataFrame(access, columns=COLUMNS_OLDKEY)
            df1 = df[(df['coverage_t'] == 'B') & (df['parcel_typ'] == 'L')]
            df1['lot_num'] = df1['lot_num'].replace([' '], '00000')
            return df1.rename(columns={'lot_num':'lot', 'lot_plan':'lotplan', 'coverage_t':'cover_typ'})

# ---- geodata preparation -----
class GDFExraction:
        """This method extact geodataframe from database.
        This method does not address the datasets derived from MapInfo reissued datasets"""
        def __init__(self, data):
            self.data = data

        # Extract tables necessary for final output with geom to geopandas df
        def query_geo(self):
            if data[0:1] == '2':
                query_geo = f"""SELECT lot, plan, lotplan, lac, loc, locality, parcel_typ, cover_typ, tenure, ca_area_sqm, o_shape
                FROM dcdb.qld_dcdb_{data} WHERE plan LIKE 'RP%%' OR plan LIKE 'SP%%' OR plan LIKE 'GTP%%' OR plan LIKE 'BUP%%'
                AND parcel_typ = 'Lot Type Parcel' AND cover_typ = 'Base' ORDER BY lotplan"""
                gdf = gpd.GeoDataFrame.from_postgis(query_geo, engine, geom_col='o_shape')
                # convert sqm to ha / create size categories
                # SQM/10000
                gdf['area_ha'] = gdf['ca_area_sqm'].astype(np.float64) / 10000
                # create size category column
                # conditions of size category
                conditions = [
                    (gdf['area_ha'] <= 1000),
                    (gdf['area_ha'] > 1000) & (gdf['area_ha'] <= 10000),
                    (gdf['area_ha'] > 10000)
                ]
                # category names to be added for each condition
                categories = [1, 2, 3]
                # create a new column and use np.select to assign categories to it using the lists as arguments
                gdf['size_cat'] = np.select(conditions, categories)
                # Delete unnecessary sqm column and reorder columns putting geometry at the end for table consistency with other ones
                gdf.pop('ca_area_sqm')
                mask = gdf.columns.isin(["o_shape"])
                return pd.concat([gdf.loc[:, ~mask], gdf.loc[:, mask]], axis=1)
            else:
                query_geo = f"""SELECT lot_num, plan_num, lot_plan, parcel_typ, coverage_t, loc_code, parish_cod, lga_code, tenure, area_ha, shape
                FROM dcdb.qld_dcdb_{data} WHERE plan_num LIKE 'RP%%' OR plan_num LIKE 'SP%%' OR plan_num LIKE 'GTP%%' OR plan_num LIKE 'BUP%%'
                AND parcel_typ = 'L' AND coverage_t = 'B' ORDER BY lot_plan"""
                gdf = gpd.GeoDataFrame.from_postgis(query_geo, engine, geom_col='shape')
                return gdf.rename(
                    columns={'lot_num': 'lot', 'plan_num': 'plan', 'lot_plan': 'lotplan', 'coverage_t': 'cover_typ',
                             'loc_code': 'loc', 'parish_cod': 'prc', 'lga_code': 'lac', 'shape': 'o_shape'})


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
    # Add 'period' column showing the period the new lots have poped up
    newlot_data['period'] = period
    gdf_p= gpd.GeoDataFrame(newlot_data, geometry='o_shape', crs="EPSG:4283")
    return gdf_p.to_postgis(f"dcdb_change_{period}", engine, index=True,
                                  index_label='id', schema='dcdb_difference', if_exists='replace')


# upload the result to the database creating a new table
def export_shp(newlot_data, period):
    newlot_data['period'] = period
    gdf_p= gpd.GeoDataFrame(newlot_data, geometry='o_shape', crs="EPSG:4283")
    return gdf_p.to_file(f"newlot_{period}.shp")


if __name__ == "__main__":
    key_pre = KeyDFCreation('21_01')
    key_new = KeyDFCreation('20_07')
    key = extract_key(key_pre, key_new)
    geodata = GDFExraction('21_07')
    result = newlot_data(key, geodata)
    upload_db(result, '2007_2101')
    print('all process finished!')

