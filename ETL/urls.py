from django.conf.urls import url
from ETL import views

urlpatterns = [
    url(r'^full_load/', views.full_load, name = 'test'),
    # url(r'^', include('orders.urls')),
]