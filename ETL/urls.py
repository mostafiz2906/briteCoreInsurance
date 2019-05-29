from django.conf.urls import url
from ETL import views

urlpatterns = [
    url(r'^full_load/', views.full_load, name = 'test'),
    url(r'^file_upload/', views.file_upload, name='file_upload'),
    # url(r'^', include('orders.urls')),
]