dim_primary_agency_column = ['PRIMARY_AGENCY_ID']
dim_agency_column = ['AGENCY_ID', 'AGENCY_APPOINTMENT_YEAR', 'ACTIVE_PRODUCERS', 'MAX_AGE',
                     'MIN_AGE', 'VENDOR_IND', 'PL_START_YEAR', 'PL_END_YEAR',
                     'COMMISIONS_START_YEAR', 'COMMISIONS_END_YEAR', 'CL_START_YEAR', 'CL_END_YEAR',
                     'ACTIVITY_NOTES_START_YEAR', 'ACTIVITY_NOTES_END_YEAR']
dim_vendor_column = ['VENDOR']
dim_time_column = ['STAT_PROFILE_DATE_YEAR']
dim_state_column = ['STATE_ABBR']
dim_product_line_column = ['PROD_LINE']
dim_product_column = ['PROD_ABBR']
fact_column = ['id_AGENCY', 'id_PRODUCT', 'id_STATE', 'id_VENDOR', 'id_TIME', 'RETENTION_POLY_QTY', 'POLY_INFORCE_QTY',
               'PREV_POLY_INFORCE_QTY', 'NB_WRTN_PREM_AMT', 'WRTN_PREM_AMT', 'PREV_WRTN_PREM_AMT',
               'PRD_ERND_PREM_AMT', 'PRD_INCRD_LOSSES_AMT', 'MONTHS', 'RETENTION_RATIO', 'LOSS_RATIO',
               'LOSS_RATIO_3YR', 'GROWTH_RATE_3YR', 'CL_BOUND_CT_MDS', 'CL_QUO_CT_MDS',
               'CL_BOUND_CT_SBZ','CL_QUO_CT_SBZ', 'CL_BOUND_CT_eQT', 'CL_QUO_CT_eQT',
               'PL_BOUND_CT_ELINKS','PL_QUO_CT_ELINKS','PL_BOUND_CT_PLRANK', 'PL_QUO_CT_PLRANK',
               'PL_BOUND_CT_eQTte', 'PL_QUO_CT_eQTte','PL_BOUND_CT_APPLIED', 'PL_QUO_CT_APPLIED',
               'PL_BOUND_CT_TRANSACTNOW', 'PL_QUO_CT_TRANSACTNOW']