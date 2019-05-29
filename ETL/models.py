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