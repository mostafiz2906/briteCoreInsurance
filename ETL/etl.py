import pandas as pd
from ETL.dim_fact_column_definition import *
from .models import *


class InitialLoad:
    base_df = pd.DataFrame()

    # default constructor
    def __init__(self,df):
        self.base_df = df

    def get_dataframe_from_filepath(self):
        return pd.read_csv(self.file_path)

    def create_dimension_from_df(self, columns):
        df_selected_column = pd.DataFrame(self.base_df, columns=columns)
        df_unique = df_selected_column.drop_duplicates()
        return df_unique


    def create_bridge_table_from_df(dim_name, input_df, *columns):
        df_selected_column = pd.DataFrame(input_df, columns=columns)
        df_unique = df_selected_column.drop_duplicates()
        return df_unique


    def create_fact(fact_name, input_base_df, input_df_vendor, input_df_state, input_df_product, df_column):
        df_fact_out = pd.merge(
            pd.merge(
                pd.merge(
                    input_base_df, input_df_vendor, on='VENDOR', how='left', suffixes=('_BASE', '_VENDOR')
                ), input_df_state, on='STATE_ABBR', how='left', suffixes=('', '_STATE')
            ), input_df_product, on='PROD_ABBR', suffixes=('', '_PRODUCT')
        ).loc[:, df_column]
        return df_fact_out

    def insert_dim_primary_agency(self,input_df):
        df_to_dict = input_df.to_dict('records')
        d_p_agency_instances = [dim_primary_agency(
            primary_agency_id=p_agency['PRIMARY_AGENCY_ID'],
        ) for p_agency in df_to_dict]
        dim_primary_agency.objects.bulk_create(d_p_agency_instances)

    def insert_dim_agency(self,input_df):
        df_to_dict = input_df.to_dict('records')
        d_agency_instances = [dim_agency(
            agency_id=agency['AGENCY_ID'],
            appointment_year=agency['AGENCY_APPOINTMENT_YEAR'],
            active_producers = agency['ACTIVE_PRODUCERS'],
            max_age =agency['MAX_AGE'],
            min_age = agency['MIN_AGE'],
            vendor_ind = agency['VENDOR_IND'],
            pl_start_year = agency['PL_START_YEAR'],
            pl_end_year = agency['PL_END_YEAR'],
            commisions_start_year =agency['COMMISIONS_START_YEAR'],
            commisions_end_year = agency['COMMISIONS_END_YEAR'],
            cl_start_year = agency['CL_START_YEAR'],
            cl_end_year = agency['CL_END_YEAR'],
            activity_notes_start_year = agency['ACTIVITY_NOTES_START_YEAR'],
            activity_notes_end_year = agency['ACTIVITY_NOTES_END_YEAR'],
        ) for agency in df_to_dict]
        dim_agency.objects.bulk_create(d_agency_instances)

    def insert_dim_vendor(self,input_df):
        df_to_dict = input_df.to_dict('records')
        d_vendor_instances = [dim_vendor(
            vendor=vendors['VENDOR'],
        ) for vendors in df_to_dict]
        dim_vendor.objects.bulk_create(d_vendor_instances)

    def insert_dim_time(self, input_df):
        df_to_dict = input_df.to_dict('records')
        d_time_instances = [dim_time(
            stat_profile_date_year=time['STAT_PROFILE_DATE_YEAR'],
        ) for time in df_to_dict]
        dim_time.objects.bulk_create(d_time_instances)

    def insert_dim_state(self, input_df):
        df_to_dict = input_df.to_dict('records')
        d_state_instances = [dim_state(
            state_abbr=state['STATE_ABBR'],
        ) for state in df_to_dict]
        dim_state.objects.bulk_create(d_state_instances)

    def insert_dim_product_line(self, input_df):
        df_to_dict = input_df.to_dict('records')
        d_p_line_instances = [dim_product_line(
            prod_line=pline['PROD_LINE'],
        ) for pline in df_to_dict]
        dim_product_line.objects.bulk_create(d_p_line_instances)

    def insert_dim_product(self, input_df):
        df_to_dict = input_df.to_dict('records')
        product_instances = [dim_product(
            prod_abbr=product['PROD_ABBR'],
        ) for product in df_to_dict]
        dim_product.objects.bulk_create(product_instances)

    def insert_bridge_product(self):
        column_from_base_df = self.base_df.loc[:, ['PROD_LINE', 'PROD_ABBR']]
        df_product_line = pd.DataFrame(list(dim_product_line.objects.all().values()))
        print df_product_line
        df_product = pd.DataFrame(list(dim_product.objects.all().values()))
        print df_product
        df_bridge_product = pd.merge(
            pd.merge(
                column_from_base_df, df_product_line, left_on='PROD_LINE', right_on='prod_line', suffixes=('_BASE', '_PRODUCT_LINE')
            ), df_product, left_on='PROD_ABBR', right_on='prod_abbr', suffixes=('_PRODUCT_LINE', '_PRODUCT')
        )
        unique_df_bridge_product =  df_bridge_product.loc[:,['id_PRODUCT_LINE','id_PRODUCT']].drop_duplicates()
        bridge_product_instances = [dim_product(
            product_line_id=row['id_PRODUCT_LINE'],
            product_id=row['id_PRODUCT'],
        )for index,row in unique_df_bridge_product.iterrows()]
        bridge_product.objects.bulk_create(bridge_product_instances)

    def insert_bridge_agency(self):
        column_from_base_df = self.base_df.loc[:, ['AGENCY_ID', 'PRIMARY_AGENCY_ID']]
        df_agency = pd.DataFrame(list(dim_agency.objects.all().values('id', 'agency_id')))
        df_primary_agnecy = pd.DataFrame(list(dim_primary_agency.objects.all().values('id', 'primary_agency_id')))
        df_bridge_agency = pd.merge(
            pd.merge(
                column_from_base_df, df_agency, left_on='AGENCY_ID', right_on='agency_id', suffixes=('_BASE', '_AGENCY')
            ), df_primary_agnecy, left_on='PRIMARY_AGENCY_ID', right_on='primary_agency_id',  suffixes=('_AGENCY', '_PRIMARY_AGNECY')
        )
        unique_df_bridge_agency =  df_bridge_agency.loc[:, ['id_AGENCY', 'id_PRIMARY_AGNECY']].drop_duplicates()
        bridge_agency_instances = [dim_product(
            agency_id=row['id_AGENCY'],
            primary_agency_id=row['id_PRIMARY_AGNECY'],
        ) for index, row in unique_df_bridge_agency.iterrows()]
        bridge_agency.objects.bulk_create(bridge_agency_instances)

    def full_load(self):
        dim_primary_agency_df = self.create_dimension_from_df( dim_primary_agency_column)
        self.insert_dim_primary_agency(dim_primary_agency_df)
        dim_agency_df = self.create_dimension_from_df(dim_agency_column)
        self.insert_dim_agency(dim_agency_df)
        dim_vendor_df = self.create_dimension_from_df(dim_vendor_column)
        self.insert_dim_vendor(dim_vendor_df)
        dim_time_df = self.create_dimension_from_df(dim_time_column)
        self.insert_dim_time(dim_time_df)
        dim_state_df = self.create_dimension_from_df(dim_state_column)
        self.insert_dim_state(dim_state_df)
        dim_product_line_df = self.create_dimension_from_df(dim_product_line_column)
        self.insert_dim_product_line(dim_product_line_df)
        dim_product_df = self.create_dimension_from_df(dim_product_column)
        self.insert_dim_product(dim_product_df)
        self.insert_bridge_product()
        self.insert_bridge_agency()



class DataframeOperation:
    file_path = ''
    # default constructor
    def __init__(self, input_path):
        self.file_path = input_path

    def get_dataframe_from_filepath(self):
        return pd.read_csv(self.file_path)

