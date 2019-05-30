# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import pandas as pd
from ETL.models import dim_product
from .forms import DocumentForm
from django.shortcuts import render
from .etl import InitialLoad, DataframeOperation
from django.conf import settings

# Create your views here.


def full_load(request):
    ## File to load
    INPUT_FILE = '/home/mostafiz/Desktop/Job/BriteCare/code/Insurance/Insurance/ETL/finalapi.csv'

    # reading data from input file/home/mostafiz/Desktop/Job/BriteCare/code/Insurance/Insurance/ETL
    base_df = pd.read_csv(INPUT_FILE)

    print base_df.columns

    # for column in base_df.columns:
    #     print type(column)

    print base_df['STATE_ABBR'].unique()

    ## get unique rows  of specified column

    # df = pd.DataFrame(base_df,columns=['AGENCY_ID','PRIMARY_AGENCY_ID'])
    # print df.drop_duplicates()
    # df.drop_duplicates().to_csv('out.csv')


    # print df.drop_duplicates().groupby('PRIMARY_AGENCY_ID').size()

    # print df_unique.loc[df_unique['PRIMARY_AGENCY_ID'] == 9550]


    def create_dimension_from_df(dim_name, input_df, append_id_flag, append_id_name, *columns):
        df_selected_column = pd.DataFrame(input_df, columns=columns)
        df_unique = df_selected_column.drop_duplicates()
        # print df_unique
        if append_id_flag:
            last_id = df_unique.shape[0] + 1
            id = [i for i in range(1, last_id)]
            # avoiding SettingWithCopyWarning
            df_unique_with_id = df_unique.copy()
            df_unique_with_id.loc[:, append_id_name] = id
            return df_unique_with_id
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

    df_primary_agency = create_dimension_from_df('DIM_PRIMARY_AGENCY', base_df, False, '', 'PRIMARY_AGENCY_ID')
    # print df_primary_agency

    df_agency = create_dimension_from_df(
        'DIM_AGENCY', base_df, False, '', 'AGENCY_ID', 'AGENCY_APPOINTMENT_YEAR', 'ACTIVE_PRODUCERS',
        'MAX_AGE', 'MIN_AGE', 'VENDOR_IND', 'PL_START_YEAR', 'PL_END_YEAR',
        'COMMISIONS_START_YEAR', 'COMMISIONS_END_YEAR', 'CL_START_YEAR', 'CL_END_YEAR',
        'ACTIVITY_NOTES_START_YEAR', 'ACTIVITY_NOTES_END_YEAR'
    )

    df_vendor = create_dimension_from_df(
        'DIM_VENDOR', base_df, True, 'VENDOR_ID', 'VENDOR'
    )

    df_time = create_dimension_from_df(
        'DIM_TIME', base_df, False, '', 'STAT_PROFILE_DATE_YEAR'
    )
    print df_time

    df_state = create_dimension_from_df('DIM_STATE', base_df, True, 'STATE_ID', 'STATE_ABBR')
    print df_state

    df_bridge_agency = create_bridge_table_from_df('BRIDGE_AGENCY', base_df, 'AGENCY_ID', 'PRIMARY_AGENCY_ID')

    df_product = create_dimension_from_df('DIM_PRODUCT_LINE', base_df, True, 'PRODUCT_ID', 'PROD_ABBR')
    for p_id,product in df_product.iterrows():
        prod = dim_product(product_id=p_id, prod_abbr=product)
        prod.save()

    df_product_line = create_dimension_from_df('DIM_PRODUCT', base_df, True, 'PRODUCT_LINE_ID', 'PROD_LINE')

    df_bridge_product_input = pd.merge(pd.merge(base_df, df_product_line, on='PROD_LINE'), df_product,
                                       on='PROD_ABBR').loc[
                              :, ['PRODUCT_LINE_ID', 'PRODUCT_ID']]
    # print df_bridge_product_input
    df_bridge_product = create_bridge_table_from_df('BRIDGE_PRODUCT', df_bridge_product_input, 'PRODUCT_LINE_ID',
                                                    'PRODUCT_ID')
    # print df_bridge_product


    base_df_columns_in_fact = ['AGENCY_ID', 'STAT_PROFILE_DATE_YEAR', 'RETENTION_POLY_QTY', 'POLY_INFORCE_QTY',
                               'PREV_POLY_INFORCE_QTY', 'NB_WRTN_PREM_AMT', 'WRTN_PREM_AMT', 'PREV_WRTN_PREM_AMT',
                               'PRD_ERND_PREM_AMT', 'PRD_INCRD_LOSSES_AMT', 'MONTHS', 'RETENTION_RATIO', 'LOSS_RATIO',
                               'LOSS_RATIO_3YR', 'GROWTH_RATE_3YR', 'CL_BOUND_CT_MDS', 'CL_QUO_CT_MDS',
                               'CL_BOUND_CT_SBZ',
                               'CL_QUO_CT_SBZ', 'CL_BOUND_CT_eQT', 'CL_QUO_CT_eQT', 'PL_BOUND_CT_ELINKS',
                               'PL_QUO_CT_ELINKS',
                               'PL_BOUND_CT_PLRANK', 'PL_QUO_CT_PLRANK', 'PL_BOUND_CT_eQTte', 'PL_QUO_CT_eQTte',
                               'PL_BOUND_CT_APPLIED', 'PL_QUO_CT_APPLIED', 'PL_BOUND_CT_TRANSACTNOW',
                               'PL_QUO_CT_TRANSACTNOW']
    df_vendor_columns_in_fact = ['VENDOR_ID']
    df_state_column_in_fact = ['STATE_ID']
    df_product_column_in_fact = ['PRODUCT_ID']
    fact_columns = base_df_columns_in_fact + df_vendor_columns_in_fact + df_state_column_in_fact + df_product_column_in_fact

    df_fact = create_fact('INSURANCE_FACT', base_df, df_vendor, df_state, df_product, fact_columns)
    #return render(request, 'home.html'



def file_upload(request):
    if request.method == 'POST':
        form = DocumentForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            media_root = settings.MEDIA_ROOT
            df_op = DataframeOperation(media_root + '/' + request.FILES['document'].name)
            base_df = df_op.get_dataframe_from_filepath()
            full_load_etl = InitialLoad(base_df)
            full_load_etl.full_load()
            return render(request, 'etl/file_upload.html')
    else:
        form = DocumentForm()
    return render(request, 'etl/file_upload.html', {
        'form': form
    })