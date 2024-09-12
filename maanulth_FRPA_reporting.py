#-------------------------------------------------------------------------------
# Name:        Maanluth Annual Reporting
#
# Purpose:     This script generates information required
#              for Maanluth Annual Reporting
#
# Input(s):    (1) BCGW connection parameters
#              (2) Reporting Year (e.g 2023)
#              (3) Workspace (folder) where outputs will be generated.
#
# Workflow:     (1) Connect to BCGW
#               (2) Execute the SQL queries
#               (3) Export the query results to spatial files
#
# Authors:      Moez Labiadh - FCBC, Nanaimo
#               Emma Armitage - GeoBC, Victoria
#
# Created:     23-01-2023
# Updated:     12-09-2024
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
import numpy as np
import pyogrio
from pyproj import Transformer

def load_queries():
    sql = {}
    
    sql['lus'] = """
                SELECT ldw.LANDSCAPE_UNIT_NAME
                FROM WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldw
                    JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                        ON SDO_RELATE(ldw.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT')= 'TRUE'
                            AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                """
    
    sql['forest_auth'] = """
        SELECT 
            frr.MAP_LABEL,
            frr.FILE_TYPE_DESCRIPTION,
            frr.FILE_STATUS_CODE,
            frr.FILE_TYPE_CODE,
            frr.LIFE_CYCLE_STATUS_CODE,
            frr.ISSUE_DATE,
            amdd.AMEND_STATUS_DATE as AMEND_DATE,
            iha.TREATY_SIDE_AGREEMENT_ID as IHA_ID, 
            
            CASE 
                WHEN amdd.AMEND_STATUS_DATE > frr.ISSUE_DATE+ 5 
                    THEN 'Amended' 
                        ELSE 'New' 
                            END AS NEW_AMEND,
                    
            frr.CURRENT_EXPIRY_DATE_CALC,
            EXTRACT(YEAR FROM frr.CURRENT_EXPIRY_DATE_CALC) - EXTRACT(YEAR FROM frr.ISSUE_DATE) AS TENURE_LENGTH_YRS,
            ROUND(SDO_GEOM.SDO_AREA(frr.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
            
            CASE 
                WHEN frr.ADMIN_DISTRICT_CODE = 'DSI' 
                    THEN 'South' 
                        ELSE 'North' 
                            END AS REGION,
                    
            ldu.LANDSCAPE_UNIT_NAME as LANDSCAPE_UNIT,
            SDO_UTIL.TO_WKTGEOMETRY(frr.GEOMETRY) SHAPE 
            
        FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AUTH_POLY_SVW frr
          
            JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                ON SDO_RELATE (frr.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                    AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                
            -- Add IHAs
            LEFT JOIN WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
                ON SDO_RELATE (iha.GEOMETRY, frr.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                    AND iha.AREA_TYPE = 'Important Harvest Area'
                    AND iha.STATUS = 'ACTIVE'
                
            -- Add Landscape Units
            JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
                ON SDO_RELATE(ldu.GEOMETRY, frr.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                AND ldu.LANDSCAPE_UNIT_NAME IN ({lus})
              
            LEFT JOIN (
                WITH CTE AS (
                        SELECT
                            amd.FOREST_FILE_ID || ' ' || amd.CUTTING_PERMIT_ID AS MAP_LABEL,
                            amd.AMEND_STATUS_DATE,
                            ROW_NUMBER() OVER (PARTITION BY amd.FOREST_FILE_ID, amd.CUTTING_PERMIT_ID ORDER BY amd.AMEND_STATUS_DATE) AS rn
                        FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AMEND amd
                        WHERE amd.AMEND_STATUS_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')
                          )
                    SELECT MAP_LABEL, AMEND_STATUS_DATE
                    FROM CTE
                    WHERE rn = 1
                ) amdd
            ON amdd.MAP_LABEL = frr.MAP_LABEL
              
            WHERE frr.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
            AND (amdd.AMEND_STATUS_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                OR 
                (frr.ISSUE_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')AND amdd.AMEND_STATUS_DATE is NULL)) 
                
          ORDER BY frr.MAP_LABEL
          """ 
    
    sql['forest_road'] = """
            SELECT ftr.MAP_LABEL,
                ftr.ROAD_SECTION_LENGTH AS ROAD_SECTION_LENGTH_KM,
                ftr.FILE_TYPE_CODE,
                ftr.FILE_TYPE_DESCRIPTION,
                ftr.FILE_STATUS_CODE,
                ftr.LIFE_CYCLE_STATUS_CODE,
                ftr.MAP_LABEL || ', Amendment ' || ftr.AMENDMENT_ID || ', Road Associated: ' || ftr.ROAD_SECTION_ID as FILE_AMEND_SECTION,
                rd.ENTRY_TIMESTAMP,
                rd.UPDATE_TIMESTAMP,
                rd.CHANGE_TIMESTAMP4,
                ftr.AWARD_DATE,
                ftr.EXPIRY_DATE,
                iha.TREATY_SIDE_AGREEMENT_ID as IHA_ID, 
                EXTRACT(YEAR FROM ftr.EXPIRY_DATE) - EXTRACT(YEAR FROM ftr.AWARD_DATE) AS TENURE_LENGTH_YRS,
                CASE 
                    WHEN ftr.AWARD_DATE > rd.CHANGE_TIMESTAMP4 + 5 
                        THEN 'New' 
                            ELSE 'Amended' 
                                END AS NEW_AMEND,
                CASE 
                    WHEN ftr.GEOGRAPHIC_DISTRICT_CODE = 'DSI' 
                        THEN 'South' 
                            ELSE 'North' 
                                END AS REGION,
                SDO_UTIL.TO_WKTGEOMETRY(rd.GEOMETRY) SHAPE 
            FROM (
                SELECT rdd.ENTRY_TIMESTAMP,
                    rdd.UPDATE_TIMESTAMP,
                    rdd.REVISION_COUNT,
                    rdd.RETIREMENT_DATE,
                    rdd.CHANGE_TIMESTAMP4,
                    rdd.UPDATE_USERID,
                    rdd.FOREST_FILE_ID || ' ' || rdd.ROAD_SECTION_ID AS MAP_LABEL,
                    rdd.GEOMETRY
                FROM WHSE_FOREST_TENURE.FTEN_ROAD_LINES rdd
                JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                    ON SDO_RELATE(rdd.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                        AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                ) rd
              
                JOIN WHSE_FOREST_TENURE.FTEN_ROAD_SECTION_LINES_SVW ftr
                    ON ftr.MAP_LABEL = rd.MAP_LABEL
                LEFT JOIN WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
                    ON SDO_RELATE(iha.GEOMETRY, ftr.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                    AND iha.AREA_TYPE = 'Important Harvest Area'
                    AND iha.STATUS = 'ACTIVE'
            WHERE ftr.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
                AND rd.RETIREMENT_DATE IS NULL
                AND (UPDATE_USERID NOT LIKE '%DATAFIX%' AND UPDATE_USERID NOT LIKE '%datafix%')
                AND (rd.CHANGE_TIMESTAMP4 BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                    OR 
                    ftr.AWARD_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY'))
            ORDER BY ftr.MAP_LABEL
            """ 
    
    sql['ftr_lu'] = """
                    SELECT
                        ftr.MAP_LABEL,
                        ftr.GEOMETRY,
                        ldm.LANDSCAPE_UNIT_NAME AS LANDSCAPE_UNIT
                    FROM WHSE_FOREST_TENURE.FTEN_ROAD_SECTION_LINES_SVW ftr
                    JOIN (
                        SELECT ldu.LANDSCAPE_UNIT_NAME,
                               ldu.GEOMETRY
                        FROM WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
                        JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                        ON SDO_RELATE(ldu.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                        AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                      ) ldm
                    ON SDO_RELATE(ftr.GEOMETRY, ldm.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                    WHERE ftr.MAP_LABEL IN ({tm})
                    """

    sql['spec_use'] = """
        SELECT supv.MAP_LABEL,
            ROUND(SDO_GEOM.SDO_AREA(supv.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
            supv.SPECIAL_USE_DESCRIPTION,
            supv.FILE_STATUS_CODE,
            supv.AMENDMENT_ID,
            iha.TREATY_SIDE_AGREEMENT_ID as IHA_ID, 
            CASE 
                WHEN supv.AMENDMENT_ID = 0
                    THEN 'New' 
                        ELSE 'Amended' 
                            END AS NEW_AMEND,
            supv.LIFE_CYCLE_STATUS_CODE,
            sup.ENTRY_TIMESTAMP,
            sup.UPDATE_TIMESTAMP,

            CASE 
                WHEN supv.ADMIN_DISTRICT_CODE = 'DSI' 
                    THEN 'South' 
                        ELSE 'North' 
                            END AS REGION,
    
            ldu.LANDSCAPE_UNIT_NAME as LANDSCAPE_UNIT,
                        
                SDO_UTIL.TO_WKTGEOMETRY(supv.GEOMETRY) SHAPE 
                  
        FROM WHSE_FOREST_TENURE.FTEN_SPEC_USE_PERMIT_POLY_SVW supv
            JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                ON SDO_RELATE (supv.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
              
            JOIN WHSE_FOREST_TENURE.FTEN_SPEC_USE_PERMIT sup
                ON sup.FOREST_FILE_ID = supv.MAP_LABEL

            -- Add IHAs
            LEFT JOIN WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
                ON SDO_RELATE (iha.GEOMETRY, supv.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                    AND iha.AREA_TYPE = 'Important Harvest Area'
                    AND iha.STATUS = 'ACTIVE'

            -- Add Landscape Units
            JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
                ON SDO_RELATE(ldu.GEOMETRY, supv.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                AND ldu.LANDSCAPE_UNIT_NAME IN ({lus})

        WHERE supv.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
            AND supv.RETIREMENT_DATE IS NULL
            AND (sup.UPDATE_USERID NOT LIKE '%DATAFIX%' AND sup.UPDATE_USERID NOT LIKE '%datafix%')
            AND sup.ENTRY_TIMESTAMP BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')

        ORDER BY supv.MAP_LABEL
        """
    
    sql['recr_poly'] = """
            SELECT rcp.MAP_LABEL,
                  ROUND(SDO_GEOM.SDO_AREA(rcpv.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
                  rcpv.FILE_STATUS_CODE,
                  rcpv.PROJECT_TYPE,
                  rcpv.LIFE_CYCLE_STATUS_CODE,
                  rcpv.PROJECT_ESTABLISHED_DATE,
                  iha.TREATY_SIDE_AGREEMENT_ID as IHA_ID,
                  CASE 
                      WHEN rcpv.PROJECT_ESTABLISHED_DATE >= rcp.CHANGE_TIMESTAMP3
                        THEN 'New' 
                          ELSE 'Amended' 
                            END AS NEW_AMEND,
                  CASE 
                    WHEN rcpv.GEOGRAPHIC_DISTRICT_CODE = 'DSI' 
                      THEN 'South' 
                        ELSE 'North' 
                          END AS REGION,
                  rcp.ENTRY_TIMESTAMP,
                  rcp.UPDATE_TIMESTAMP,
                  rcp.CHANGE_TIMESTAMP3,
                  ldu.LANDSCAPE_UNIT_NAME as LANDSCAPE_UNIT,
                  SDO_UTIL.TO_WKTGEOMETRY(rcpv.GEOMETRY) SHAPE 

            FROM (
                SELECT  rcpp.FOREST_FILE_ID as MAP_LABEL,
                        rcpp.RETIREMENT_DATE,
                        rcpp.ENTRY_USERID,  
                        rcpp.UPDATE_USERID,
                        rcpp.ENTRY_TIMESTAMP,
                        rcpp.UPDATE_TIMESTAMP,
                        rcpp.CHANGE_TIMESTAMP3
                  
                FROM WHSE_FOREST_TENURE.FTEN_RECREATION_POLY rcpp
                    JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                        ON SDO_RELATE (rcpp.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                        AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                    )rcp
                  
                    JOIN WHSE_FOREST_TENURE.FTEN_RECREATION_POLY_SVW rcpv
                    ON rcp.MAP_LABEL = rcpv.FOREST_FILE_ID

                    -- Add IHAs
                    LEFT JOIN WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
                        ON SDO_RELATE (iha.GEOMETRY, rcpv.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                            AND iha.AREA_TYPE = 'Important Harvest Area'
                            AND iha.STATUS = 'ACTIVE'

                    -- Add Landscape Units
                    JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
                        ON SDO_RELATE(ldu.GEOMETRY, rcpv.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                        AND ldu.LANDSCAPE_UNIT_NAME IN ({lus})
              
                WHERE rcpv.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
                    AND rcp.RETIREMENT_DATE IS NULL
                    AND (rcp.UPDATE_USERID NOT LIKE '%DATAFIX%' AND rcp.UPDATE_USERID NOT LIKE '%datafix%')
                    AND (rcp.CHANGE_TIMESTAMP3 BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                        OR 
                        rcpv.PROJECT_ESTABLISHED_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY'))

                ORDER BY rcp.MAP_LABEL
                """ 
    
    sql['recr_line'] = """
            SELECT rcp.MAP_LABEL,
                rcpv.FEATURE_LENGTH AS LENGTH_KM,
                rcpv.FILE_STATUS_CODE,
                rcpv.PROJECT_TYPE,
                rcpv.LIFE_CYCLE_STATUS_CODE,
                rcpv.PROJECT_ESTABLISHED_DATE,
                iha.TREATY_SIDE_AGREEMENT_ID as IHA_ID,
                CASE 
                    WHEN rcpv.PROJECT_ESTABLISHED_DATE >= rcp.CHANGE_TIMESTAMP3
                      THEN 'New' 
                        ELSE 'Amended' 
                          END AS NEW_AMEND,
                CASE 
                  WHEN rcpv.DISTRICT_CODE = 'DSI' 
                    THEN 'South' 
                      ELSE 'North' 
                        END AS REGION,
                rcp.ENTRY_TIMESTAMP,
                rcp.UPDATE_TIMESTAMP,
                rcp.CHANGE_TIMESTAMP3,
                ldu.LANDSCAPE_UNIT_NAME as LANDSCAPE_UNIT,
                SDO_UTIL.TO_WKTGEOMETRY(rcpv.GEOMETRY) SHAPE 

            FROM (
                SELECT  rcpp.FOREST_FILE_ID || ' ' || rcpp.SECTION_ID AS MAP_LABEL,
                        rcpp.RETIREMENT_DATE,
                        rcpp.ENTRY_USERID,  
                        rcpp.UPDATE_USERID,
                        rcpp.ENTRY_TIMESTAMP,
                        rcpp.UPDATE_TIMESTAMP,
                        rcpp.CHANGE_TIMESTAMP3
                
                FROM WHSE_FOREST_TENURE.FTEN_RECREATION_LINE rcpp
                JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                    ON SDO_RELATE (rcpp.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                        AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                )rcp
                
                JOIN WHSE_FOREST_TENURE.FTEN_RECREATION_LINES_SVW rcpv
                ON rcp.MAP_LABEL = rcpv.MAP_LABEL

                -- Add IHAs
                LEFT JOIN WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
                    ON SDO_RELATE (iha.GEOMETRY, rcpv.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                        AND iha.AREA_TYPE = 'Important Harvest Area'
                        AND iha.STATUS = 'ACTIVE'

                -- Add Landscape Units
                JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
                    ON SDO_RELATE(ldu.GEOMETRY, rcpv.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                    AND ldu.LANDSCAPE_UNIT_NAME IN ({lus})
            
            WHERE rcpv.LIFE_CYCLE_STATUS_CODE = 'ACTIVE'
                AND rcp.RETIREMENT_DATE IS NULL
                AND (rcp.UPDATE_USERID NOT LIKE '%DATAFIX%' AND rcp.UPDATE_USERID NOT LIKE '%datafix%')
                AND (rcp.CHANGE_TIMESTAMP3 BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                    OR 
                    rcpv.PROJECT_ESTABLISHED_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY'))

            ORDER BY rcp.MAP_LABEL
            """ 
    
    return sql

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("...Successfuly connected to the database")
    
    except:
        raise Exception('...Connection failed! Please verifiy your login parameters')
    
    return connection

def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
        
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf

def get_lus(k, v, connection):
        """Returns a list of Landscape Units that overlap with Maa'Nulth boundaries"""

        print(f"..executing query for Landscape Unit Names: {k}")
        query = v
        df_lus = pd.read_sql(query, connection)
        
        # extract the LANDSCAPE_UNIT_NAME into a list to use in the auth queries
        lus_list = df_lus['LANDSCAPE_UNIT_NAME'].tolist()
        
        # format lus_list
        lus = ",".join(f"'{lu}'" for lu in lus_list)

        return lus    

def execute_queries(k, v, sql, year, connection, lus):
    """Executes SQL authorization queries"""
    # initialize empty dataframes
    df_geo = pd.DataFrame()
    df_tbl = pd.DataFrame()
    ftr_lu_tbl = pd.DataFrame()

    print(f"\n..working on SQL {k}")
    query = v.format(y= year, prvy=year-1, lus=lus)
    
    # read the query into a dataframe
    print ("....executing the query")
    df_geo = pd.read_sql(query,connection)

    # check if dataframe is empty 
    if not df_geo.empty:
    
        if 'SHAPE' in df_geo.columns:
            df_tbl = df_geo.drop(['SHAPE'], axis=1)
        else:
            df_tbl = df_geo

        # Execute the 'forest_road' query using the get_lu_overlaps_ftr function - used to optimize query performance
        if k == 'forest_road' and df_tbl is not None:
            print(f"....executing forest road query: {k}")
            ftr_lu_tbl = get_lu_overlaps_ftr(df_tbl=df_tbl, connection=connection, sql=sql, year=year)

    else:
        print(f"..query {k} returned an empty dataframe - no report will be produced")
        
    return df_geo, df_tbl, ftr_lu_tbl

def execute_road_lu_query(connection, query):
    """Returns a dataframe of road authorizations and their Landscape Unit Overlaps"""
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()       
            
def get_lu_overlaps_ftr(df_tbl, connection, sql, year):
    """Returns a dataframe containing overlaps of Landscape Units for forest road authorizations"""
    ftr_map_labels = ",".join("'" + str(x) + "'" for x in df_tbl['MAP_LABEL'].tolist())
    
    query = sql['ftr_lu'].format(tm=ftr_map_labels)
    
    df_lu = execute_road_lu_query(connection, query)
    df_lu = df_lu.groupby(['MAP_LABEL'])['LANDSCAPE_UNIT'].apply(lambda x: ', '.join(map(str, x))).reset_index()
    
    return df_lu

def df_to_gdf(df, crs):
    """Returns a geopandas gdf based on a df with Geometry column"""
    
    df['SHAPE'] = df['SHAPE'].astype(str)
    
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    
    gdf.crs = "EPSG:" + str(crs)
    
    del gdf['SHAPE']
    
    return gdf

def get_fn_overlaps(gdf, fn_fc):
    """Returns a df containing overlaps with individual Maanulth First Nations"""
    
    gdf_fn = esri_to_gdf(fn_fc)
    gdf_intersect = gpd.overlay(gdf, gdf_fn, how='intersection')
    
    return gdf_intersect

def group_fn_overlaps(gdf_fn):
    """
    Groups all First Nation overlaps into one line per MAP_LABEL
    
    Returns the grouped dataframe
    """
    # bring all First Nation overlaps on to one line
    gdf_fn_one_line = gdf_fn.groupby('MAP_LABEL')['FN_area_r'].agg(lambda x: ' & '.join(map(str, set(x)))).reset_index()

    # merge back with the original dataframe
    gdf_fn = pd.merge(gdf_fn, gdf_fn_one_line, how='left', on='MAP_LABEL')
    
    return gdf_fn

def generate_report(workspace, df_tbl, sheet, filename):
    """Exports dataframes to a multi-tab excel spreadsheet"""

    file_name = os.path.join(workspace, f"{filename}.xlsx")

    # check if file exists
    if os.path.exists(file_name):
        with pd.ExcelWriter(file_name, engine='openpyxl', mode='a', if_sheet_exists='new') as writer:
            df_tbl.to_excel(writer, sheet_name=sheet, index=False, startrow=0, startcol=0)
    else:
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            df_tbl.to_excel(writer, sheet_name=sheet, index=False, startrow=0, startcol=0)       

# Function to convert a list of coordinates
transformer = Transformer.from_crs("EPSG:3857", "EPSG: 4326", always_xy=True)

def generate_spatial_files(gdf, workspace, year, k):
    """Generate a GeoJSON of authorizations"""

    geojson_name = os.path.join(workspace, f"maanulth_{k}_{str(year)}_shapes.geojson")

    # reproject coordinates to Web Mercator
    gdf.to_crs(crs="EPSG:3857", inplace=True)

    for col in gdf.columns:
        if 'DATE' in col.upper():
            gdf[col] = gdf[col].apply(lambda x: None if pd.isna(x) else str(x))

    gdf.to_file(geojson_name, driver='GeoJSON')

def main():
    print ('\nConnecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = '' ################## CHANGE THIS################
    bcgw_pwd = '' ################## CHANGE THIS################
    
    connection = connect_to_DB(hostname=hostname, username=bcgw_user, password=bcgw_pwd)

    # reporting year
    year = 2024 ################## CHANGE THIS################
    
    print ("\nLoad the SQL queries...")
    sql = load_queries()
    
    print ("\nRun the process")
    workspace = r'' ################## CHANGE THIS################
    
    # landscape units geodatabase
    fn_fc= r'\\spatialfiles.bcgov\work\lwbc\visr\Workarea\moez_labiadh\DATASETS\Maa-nulth.gdb\PreTreatyFirstNationAreas'

    # initialize dictionary - to be written to excel later
    report_dict = {}

    # initialize lus variables
    lus = None
    # execute the sql queries and return the resulting dataframe
    for k, v in sql.items():
        if k == 'lus':
            lus = get_lus(k, v, connection)
            continue
        elif k != 'ftr_lu':
            df_geo, df_tbl, ftr_lu_tbl = execute_queries(k=k, v=v, sql=sql, year=year, connection=connection, lus=lus)

            if not df_geo.empty and not df_tbl.empty:
                # convert geo_df to geodataframe
                gdf = df_to_gdf(df=df_geo, crs=3005)
                gdf.drop_duplicates(subset=['MAP_LABEL'], inplace=True)

                # get overlaps w/ individual Maa'Nulth First Nations
                gdf_fn = get_fn_overlaps(gdf=gdf, fn_fc=fn_fc)

                print("\nCleaning up results...")
                # group IHA IDs into a single row 
                df_iha = df_tbl[['MAP_LABEL', 'IHA_ID']]
                df_iha['IHA_ID'] = pd.to_numeric(df_tbl['IHA_ID'], errors='coerce').fillna(0).astype(int)
                df_iha = df_iha.groupby('MAP_LABEL')['IHA_ID'].agg(lambda x: '; '.join(map(str, set(x)))).reset_index()
                
                df_tbl.drop(columns=['IHA_ID'], inplace= True)
                df_tbl = pd.merge(df_tbl, df_iha, how='left', on= 'MAP_LABEL')

                # get a list of column names. allows differentiate dataframes
                df_columns = df_tbl.columns.to_list()

                if 'LANDSCAPE_UNIT' in df_columns:
                    # group LANDSCAPE_UNIT into a single row
                    df_lu = df_tbl[['MAP_LABEL', 'LANDSCAPE_UNIT']]
                    df_lu = df_lu.groupby('MAP_LABEL')['LANDSCAPE_UNIT'].agg(lambda x: '; '.join(map(str, set(x)))).reset_index()
                    
                    df_tbl.drop(columns=['LANDSCAPE_UNIT'], inplace=True)
                    df_tbl = pd.merge(df_tbl, df_lu, how='left', on='MAP_LABEL')

                    df_tbl.drop_duplicates(subset=['MAP_LABEL', 'LANDSCAPE_UNIT'], inplace=True)

                if 'LANDSCAPE_UNIT' not in df_columns:
                    df_tbl = pd.merge(df_tbl, ftr_lu_tbl, how='left', on='MAP_LABEL')
                
                # replace areas w/ no IHA overlaps w/ None
                df_tbl['IHA_ID'] = df_tbl['IHA_ID'].replace({'0': None})
                
                # add First Nation info to the main dataframe
                gdf_fn.rename(columns={'FN_area_r': 'FN'}, inplace=True)
                gdf_fn = gdf_fn.groupby('MAP_LABEL')['FN'].agg(lambda x: ' & '.join(map(str, set(x)))).reset_index()
                
                df_tbl = pd.merge(df_tbl, gdf_fn, how='left', on='MAP_LABEL')
                
                print("\nCleaning up columns...")
                df_tbl['AGENCY'] = 'FOR'
                df_tbl['LEGISLATION'] = 'Forest Act and FRPA'
                df_tbl['SPATIAL'] = 'Yes'
                df_tbl['LAT_LONG'] = None
                df_tbl['IS_IHA'] = None
                df_tbl['DID_ENGAGE_OCCUR'] = 'Enter Yes or No'
                df_tbl['IF_NO_ENGAGE'] = None
                df_tbl['AMEND_DATE'] = None

                if k in ['recr_poly', 'recr_line']:
                    df_tbl['FILE_TYPE_CODE'] = None

                # populate IS_IHA
                df_tbl['IS_IHA'] = np.where(df_tbl['IHA_ID'].notna(), 'YES', 'NO')

                if 'TENURE_LENGTH_YRS' in df_columns:
                    # replace values per Annual reporting template
                    df_tbl['TENURE_LENGTH_YRS'] = np.where(df_tbl['TENURE_LENGTH_YRS'] == 0, 1, df_tbl['TENURE_LENGTH_YRS'])
                    df_tbl['TENURE_LENGTH_YRS']= df_tbl['TENURE_LENGTH_YRS'].fillna(9999)
                    df_tbl['TENURE_LENGTH_YRS']= df_tbl['TENURE_LENGTH_YRS'].astype(int).astype(str)
                    df_tbl['TENURE_LENGTH_YRS'] = df_tbl['TENURE_LENGTH_YRS'].replace({'9999': 'N/A'})
                else:
                    df_tbl['TENURE_LENGTH_YRS'] = None
                    df_tbl['TENURE_LENGTH_YRS']= df_tbl['TENURE_LENGTH_YRS'].fillna(9999)
                    df_tbl['TENURE_LENGTH_YRS']= df_tbl['TENURE_LENGTH_YRS'].astype(int).astype(str)
                    df_tbl['TENURE_LENGTH_YRS'] = df_tbl['TENURE_LENGTH_YRS'].replace({'9999': 'N/A'})

                
                # reorder columns for each dataset
                if k in ['forest_auth', 'spec_use']:
                    cols= ['REGION', 'LANDSCAPE_UNIT', 'MAP_LABEL', 'AGENCY', 'LEGISLATION', 'FILE_TYPE_DESCRIPTION',
                           'FILE_STATUS_CODE', 'FILE_TYPE_CODE', 'NEW_AMEND', 'ISSUE_DATE', 'TENURE_LENGTH_YRS',
                           'AREA_HA', 'SPATIAL', 'LAT_LONG', 'IS_IHA', 'IHA_ID', 'DID_ENGAGE_OCCUR', 'IF_NO_ENGAGE', 'FN', 'AMEND_DATE']   

                    df_tbl = df_tbl[cols]

                if k in ['forest_road']:
                    cols= ['REGION', 'LANDSCAPE_UNIT', 'MAP_LABEL', 'FILE_AMEND_SECTION', 'AGENCY', 'LEGISLATION', 'FILE_TYPE_DESCRIPTION',
                           'FILE_STATUS_CODE', 'FILE_TYPE_CODE', 'NEW_AMEND', 'ENTRY_TIMESTAMP', 'TENURE_LENGTH_YRS',
                           'ROAD_SECTION_LENGTH_KM', 'SPATIAL', 'LAT_LONG', 'IS_IHA', 'IHA_ID', 'DID_ENGAGE_OCCUR', 'IF_NO_ENGAGE', 'FN', 'AMEND_DATE']   

                    df_tbl = df_tbl[cols]         

                if k in ['recr_poly']:
                    cols= ['REGION', 'LANDSCAPE_UNIT', 'MAP_LABEL', 'AGENCY', 'LEGISLATION', 'PROJECT_TYPE',
                           'FILE_STATUS_CODE', 'FILE_TYPE_CODE', 'NEW_AMEND', 'ENTRY_TIMESTAMP', 'TENURE_LENGTH_YRS',
                           'AREA_HA', 'SPATIAL', 'LAT_LONG', 'IS_IHA', 'IHA_ID', 'DID_ENGAGE_OCCUR', 'IF_NO_ENGAGE', 'FN', 'AMEND_DATE']   

                    df_tbl = df_tbl[cols]

                if k in ['recr_line']:
                    cols= ['REGION', 'LANDSCAPE_UNIT', 'MAP_LABEL', 'AGENCY', 'LEGISLATION', 'PROJECT_TYPE',
                           'FILE_STATUS_CODE', 'FILE_TYPE_CODE', 'NEW_AMEND', 'ENTRY_TIMESTAMP', 'TENURE_LENGTH_YRS',
                           'LENGTH_KM', 'SPATIAL', 'LAT_LONG', 'IS_IHA', 'IHA_ID', 'DID_ENGAGE_OCCUR', 'IF_NO_ENGAGE', 'FN', 'AMEND_DATE']   

                    df_tbl = df_tbl[cols]

                df_tbl.sort_values(by='MAP_LABEL', inplace=True)

                # add resulting dataframe to dictionary
                report_dict[k] = df_tbl

                print(f"\nExporting {k} to spatial file...")
                # add geometry column to df_tbl
                geom_column = df_geo[['MAP_LABEL', 'geometry']]
                output_df = pd.merge(df_tbl, geom_column, how="left", on='MAP_LABEL')
                
                # convert to gdf
                output_gdf = gpd.GeoDataFrame(output_df, geometry=output_df['geometry'], crs=3005)
                output_gdf.drop_duplicates(subset=['MAP_LABEL'], inplace=True)

                # export to GeoJSON
                generate_spatial_files(output_gdf, workspace, year, k)


    print("\nExporting to Excel...")
    for key, value in report_dict.items():
        sheet = key
        filename = f'Maanulth_FRPA_annualReporting_tables_{str(year)}'

        if key == 'ftr_lu':
            continue
        else:
            generate_report(workspace, value, sheet, filename)

    print ("\nProcessing Completed!")
    
if __name__ == '__main__':
    main()
