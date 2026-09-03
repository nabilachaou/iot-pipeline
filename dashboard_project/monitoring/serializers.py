

from rest_framework import serializers

from . import models


def make_serializer(model_class):
    """Construit un ModelSerializer exposant tous les champs du modèle."""

    meta_attrs = {"model": model_class, "fields": "__all__"}
    meta_class = type("Meta", (), meta_attrs)
    serializer_name = f"{model_class.__name__}Serializer"

    return type(serializer_name, (serializers.ModelSerializer,), {"Meta": meta_class})


RejectedDataSerializer = make_serializer(models.RejectedData)
FactRejectedHourlySerializer = make_serializer(models.FactRejectedHourly)
