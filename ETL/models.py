# -*- coding: utf-8 -*-
from django.db import models

# Create your models here.


class dim_product (models.Model):
    product_id = models.IntegerField()
    prod_abbr = models.CharField(max_length=100)