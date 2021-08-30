## Update multiple tables at one time

#packages
import psycopg2
from secret import connect
from secret import periods, periods2

#Variable
conn = connect
cur = conn.cursor()

# Update SRID
def update_SRID_bulk(periods):
    '''does not work somehow for now, need to check what is wrong'''
    for i in range(len(periods) - 1):
        query = f"""SELECT UpdateGeometrySRID('dcdb_difference','dcdb_change_{periods[i]}', 'shape', 4283)"""
        cur.execute(query)
        conn.commit()
        print(cur.fetchall())


# Check SRID
def check_SRID_bulk(periods):
    for i in range(len(periods)):
        query = f"""SELECT Find_SRID('dcdb_difference','dcdb_change_{periods[i]}', 'shape')"""
        cur.execute(query)
        print(periods[i], cur.fetchall())


# Update column
def update_column_bulk(periods, COLUMNS_NEWKEY_F, COLUMNS_OLDKEY_F):
    '''rename column names in bulk'''
    for i in range(len(periods)):
        for j in range(len(COLUMNS_OLDKEY_F)):
            query = f"""ALTER TABLE dcdb_difference.dcdb_change_{periods[i]} RENAME COLUMN {COLUMNS_OLDKEY_F[j]} TO {COLUMNS_NEWKEY_F[j]}"""
            cur.execute(query)
            conn.commit()
            print(periods[i])


# Drop column
def drop_column_bulk(periods, dropping_column):
    '''drop column names in bulk'''
    for i in range(len(periods)):
        for j in range(len(dropping_column)):
            query = f"""ALTER TABLE dcdb_difference.dcdb_change_{periods[i]} DROP COLUMN {dropping_column[j]}"""
            cur.execute(query)
            conn.commit()
            print(periods[i])


# Update tenure
def update_tenure_bulk(periods, tenure_new, tenure_old):
    '''rename tenure column unique values in bulk'''
    for i in range(len(periods)):
        for j in range(len(tenure_new)):
            query = f"""UPDATE dcdb_difference.dcdb_change_{periods[i]} SET tenure = '{tenure_new[j]}' WHERE tenure = '{tenure_old[j]}'"""
            cur.execute(query)
            conn.commit()
            print(periods[i])


if __name__ == "__main__":
    # Periods
    periods = periods
    # rename columns in bulk - only choose 'eixting names' as no 'IF EXISTS' function in postgis for rename column
    COLUMNS_NEWKEY_F = ['lot', 'plan', 'lotplan', 'parcel_typ', 'cover_typ', 'loc', 'prc', 'lac', 'tenure', 'area_ha',
                        'size_cat', 'o_shape']
    COLUMNS_OLDKEY_F = ["lot_num", "plan_num", "lot_plan", "parcel_typ", "coverage_t", "loc_code", "parish_cod",
                        "lga_code", "tenure", "area_ha", "size_cat", "shape"]
    # update tenure unique values
    tenure_new = ['Airport', 'Commonwealth Acquisition', 'Covenant', 'Below The Depth Plans', 'Freehold',
                  'Housing Land',
                  'Boat Harbours', 'Industrial Estates', 'Lands Lease', 'Main Road', 'Mines Tenure', 'National Park',
                  'Port And Harbour Boards',
                  'Reserve', 'Railway', 'State Forest', 'State Land', 'Transferred Property', 'Water Resource']
    tenure_old = ['AP', 'CA', 'CV', 'FD', 'FH', 'HL',
                  'HM', 'ID', 'LL', 'MR', 'MT', 'NP', 'PH',
                  'RE', 'RY', 'SF', 'SL', 'TP', 'WR']

    # dropping column
    dropping_column = ['shape_length', 'shape_area']

    # Exectute
    # update_column_bulk(periods,COLUMNS_NEWKEY_F, COLUMNS_OLDKEY_F)
    # update_tenure_bulk(periods,tenure_new, tenure_old)
    drop_column_bulk(periods3, dropping_column)