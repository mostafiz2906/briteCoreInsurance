# -*- coding: utf-8 -*-
# Generated by Django 1.10.1 on 2019-05-31 08:30
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ETL', '0016_dim_product_test'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dim_product',
            name='test',
        ),
    ]
