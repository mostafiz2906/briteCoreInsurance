# -*- coding: utf-8 -*-
# Generated by Django 1.10.1 on 2019-05-28 14:40
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ETL', '0009_input_file'),
    ]

    operations = [
        migrations.AlterField(
            model_name='input_file',
            name='document',
            field=models.FileField(upload_to=b''),
        ),
    ]