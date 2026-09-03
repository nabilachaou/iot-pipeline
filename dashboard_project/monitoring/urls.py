from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard_view, name="dashboard"),

    path("api/<str:equipment_type>/equipment-ids/", views.equipment_ids, name="equipment-ids"),
    path("api/<str:equipment_type>/latest/", views.latest_data, name="latest-data"),
    path("api/<str:equipment_type>/hourly/", views.hourly_data, name="hourly-data"),

    path("api/rejected/summary/", views.rejected_summary, name="rejected-summary"),
    path("api/rejected/recent/", views.rejected_recent, name="rejected-recent"),
]
