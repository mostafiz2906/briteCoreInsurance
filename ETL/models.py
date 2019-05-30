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

class dim_product (models.Model):
    product_id = models.IntegerField()
    prod_abbr = models.CharField(max_length=100)

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
