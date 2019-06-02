# -*- coding: utf-8 -*-
from django.db import models

# Create your models here.


def user_directory_path(instance, filename):
    # file will be uploaded to MEDIA_ROOT/user_<id>/<filename>
    return 'user_{0}/{1}'.format(instance.user.id, filename)


class input_file(models.Model):
    file_name = models.CharField(max_length=255)
    document = models.FileField()
    uploaded_at = models.DateTimeField(auto_now_add=True)


class dim_primary_agency (models.Model):
    primary_agency_id = models.IntegerField()

class dim_agency(models.Model):
    agency_id = models.IntegerField()
    appointment_year = models.IntegerField()
    active_producers = models.IntegerField()
    max_age = models.IntegerField()
    min_age = models.IntegerField()
    vendor_ind = models.CharField(max_length=10)
    pl_start_year = models.IntegerField()
    pl_end_year = models.IntegerField()
    commisions_start_year = models.IntegerField()
    commisions_end_year = models.IntegerField()
    cl_start_year = models.IntegerField()
    cl_end_year = models.IntegerField()
    activity_notes_start_year = models.IntegerField()
    activity_notes_end_year = models.IntegerField()

class dim_vendor(models.Model):
    vendor = models.CharField(max_length=50)

class dim_time(models.Model):
    stat_profile_date_year = models.IntegerField()

class dim_state(models.Model):
    state_abbr = models.CharField(max_length=5)

class dim_product_line(models.Model):
    prod_line = models.CharField(max_length=5)

class dim_product(models.Model):
    PROD_ABBR = models.CharField(max_length=30)

class bridge_agency(models.Model):
    agency = models.ForeignKey(dim_agency)
    primary_agency = models.ForeignKey(dim_primary_agency)

class bridge_product(models.Model):
    product_line = models.ForeignKey(dim_product_line)
    product = models.ForeignKey(dim_product)

class fact_insurance(models.Model):
    agency = models.ForeignKey(dim_agency)
    product = models.ForeignKey(dim_product)
    state = models.ForeignKey(dim_state)
    vendor = models.ForeignKey(dim_vendor)
    time = models.ForeignKey(dim_time)
    retention_poly_qty = models.DecimalField(max_digits=30, decimal_places=10)
    poly_inforce_qty = models.DecimalField(max_digits=30, decimal_places=10)
    prev_poly_inforce_qty = models.DecimalField(max_digits=30, decimal_places=10)
    nb_wrtn_prem_amnt = models.DecimalField(max_digits=30, decimal_places=10)
    wrtn_prem_amnt = models.DecimalField(max_digits=30, decimal_places=10)
    prd_ernd_prem_amt = models.DecimalField(max_digits=30, decimal_places=10)
    prd_incr_losses_amt = models.DecimalField(max_digits=30, decimal_places=10)
    months = models.DecimalField(max_digits=30, decimal_places=10)
    retention_ratio = models.DecimalField(max_digits=30, decimal_places=10)
    loss_ratio = models.DecimalField(max_digits=30, decimal_places=10)
    loss_ratio_3yr = models.DecimalField(max_digits=30, decimal_places=10)
    growth_rate_3yr = models.DecimalField(max_digits=30, decimal_places=10)
    cl_bound_ct_mds = models.DecimalField(max_digits=30, decimal_places=10)
    cl_quo_ct_mds = models.DecimalField(max_digits=30, decimal_places=10)
    cl_bound_ct_sbz = models.DecimalField(max_digits=30, decimal_places=10)
    cl_quo_ct_sbz = models.DecimalField(max_digits=30, decimal_places=10)
    cl_bound_ct_eqt = models.DecimalField(max_digits=30, decimal_places=10)
    cl_quo_ct_eqt = models.DecimalField(max_digits=30, decimal_places=10)
    pl_bound_ct_elinks = models.DecimalField(max_digits=30, decimal_places=10)
    pl_quo_ct_elinks = models.DecimalField(max_digits=30, decimal_places=10)
    pl_bound_ct_plrank = models.DecimalField(max_digits=30, decimal_places=10)
    pl_quo_ct_plrank = models.DecimalField(max_digits=30, decimal_places=10)
    pl_bound_ct_eqtte = models.DecimalField(max_digits=30, decimal_places=10)
    pl_quo_ct_eqtte = models.DecimalField(max_digits=30, decimal_places=10)
    pl_bound_ct_applied = models.DecimalField(max_digits=30, decimal_places=10)
    pl_quo_ct_applied = models.DecimalField(max_digits=30, decimal_places=10)
    pl_bound_ct_transactnow = models.DecimalField(max_digits=30, decimal_places=10)
    pl_quo_ct_transactnow = models.DecimalField(max_digits=30, decimal_places=10)
