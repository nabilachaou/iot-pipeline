```python


from datetime import timedelta

from django.shortcuts import render
from django.utils import timezone
from django.db.models import Avg, Min, Max, Sum, Q

from rest_framework.decorators import api_view
from rest_framework.response import Response

from . import models, serializers
from .registry import EQUIPMENT_REGISTRY, get_equipment_config


# ============================================================
# UTILITAIRE
# ============================================================

def get_positive_int(request, parameter, default, minimum=1, maximum=1000):
    """
    Récupère proprement un paramètre entier depuis l'URL.

    Exemple :
        ?limit=100
        ?hours=24

    Si la valeur est absente ou invalide, la valeur par défaut est utilisée.
    """
    try:
        value = int(request.GET.get(parameter, default))
    except (TypeError, ValueError):
        value = default

    return max(minimum, min(value, maximum))


# ============================================================
# PAGE HTML
# ============================================================

def dashboard_view(request):
    """
    Page principale du tableau de bord.

    La liste des types d'équipements est envoyée au template.
    Les données sont ensuite chargées dynamiquement via les endpoints
    /api/.
    """

    equipment_choices = [
        {
            "key": key,
            "label": cfg["label"]
        }
        for key, cfg in EQUIPMENT_REGISTRY.items()
    ]

    return render(
        request,
        "monitoring/dashboard.html",
        {
            "equipment_choices": equipment_choices
        }
    )


# ============================================================
# API -- IDENTIFIANTS DES ÉQUIPEMENTS
# ============================================================

@api_view(["GET"])
def equipment_ids(request, equipment_type):
    """
    GET /api/<equipment_type>/equipment-ids/

    Retourne les identifiants distincts des équipements disponibles
    dans la table *_data_clean.
    """

    config = get_equipment_config(equipment_type)

    if config is None:
        return Response(
            {
                "error": f"Type d'équipement inconnu : {equipment_type}"
            },
            status=404
        )

    ids = (
        config["clean_model"]
        .objects
        .values_list("equipment_id", flat=True)
        .distinct()
        .order_by("equipment_id")
    )

    return Response(list(ids))


# ============================================================
# API -- DONNÉES TEMPS RÉEL
# ============================================================

@api_view(["GET"])
def latest_data(request, equipment_type):
    """
    GET /api/<equipment_type>/latest/?equipment_id=...&limit=100

    Retourne les dernières mesures nettoyées.

    Les résultats sont retournés dans l'ordre chronologique croissant
    afin d'être directement exploitables par Chart.js.
    """

    config = get_equipment_config(equipment_type)

    if config is None:
        return Response(
            {
                "error": f"Type d'équipement inconnu : {equipment_type}"
            },
            status=404
        )

    limit = get_positive_int(
        request,
        "limit",
        default=100,
        minimum=1,
        maximum=1000
    )

    queryset = config["clean_model"].objects.all()

    # --------------------------------------------------------
    # Filtre équipement
    # --------------------------------------------------------

    equipment_id = request.GET.get("equipment_id")

    if equipment_id and equipment_id != "all":
        queryset = queryset.filter(
            equipment_id=equipment_id
        )

    # --------------------------------------------------------
    # Les modèles *_clean sont triés par date décroissante
    # dans Meta.ordering.
    #
    # On récupère d'abord les N plus récentes,
    # puis on inverse pour obtenir :
    #
    # ancien -> récent
    # --------------------------------------------------------

    rows = list(
        queryset[:limit]
    )

    rows.reverse()

    serializer_class = serializers.make_serializer(
        config["clean_model"]
    )

    serialized = serializer_class(
        rows,
        many=True
    ).data

    return Response(
        {
            "equipment_type": equipment_type,
            "numeric_fields": config["numeric_fields"],
            "status_field": config["status_field"],
            "count": len(serialized),
            "results": serialized,
        }
    )


# ============================================================
# API -- AGRÉGATS HORAIRES
# ============================================================

@api_view(["GET"])
def hourly_data(request, equipment_type):
    """
    GET /api/<equipment_type>/hourly/?equipment_id=...&hours=48

    Retourne les agrégats horaires provenant des tables :

        fact_elevator_hourly
        fact_rmg_hourly
        fact_rtg_hourly
        fact_sts_hourly
        fact_straddle_hourly
        fact_tractor_hourly

    Les données sont déjà calculées par l'ETL SQL.

    CORRIGÉ :
    Lorsque equipment_id="all" (ou absent), la table de faits
    contient une ligne par (equipment_key, date_key, hour_key).

    Ainsi, plusieurs équipements actifs pendant la même heure
    peuvent produire plusieurs lignes avec le même (date_key, hour_key).

    On regroupe donc explicitement les équipements par :

        (date_key, hour_key)

    afin d'obtenir une seule ligne par heure pour Chart.js.

    Agrégations utilisées :

        avg_*                  -> Avg
        min_*                  -> Min
        max_*                  -> Max
        measurement_count      -> Sum
        alarm_count            -> Sum
        fault_count            -> Sum

    Lorsqu'un équipement précis est sélectionné, le comportement
    précédent est conservé.
    """

    config = get_equipment_config(equipment_type)

    if config is None:
        return Response(
            {
                "error": f"Type d'équipement inconnu : {equipment_type}"
            },
            status=404
        )

    hours = get_positive_int(
        request,
        "hours",
        default=48,
        minimum=1,
        maximum=24 * 30
    )

    model = config["fact_model"]

    queryset = (
        model
        .objects
        .select_related("equipment_key")
    )

    # --------------------------------------------------------
    # Filtre équipement
    # --------------------------------------------------------

    equipment_id = request.GET.get("equipment_id")

    if equipment_id and equipment_id != "all":

        # --------------------------------------------------------
        # Cas équipement unique :
        # comportement inchangé.
        # --------------------------------------------------------

        queryset = queryset.filter(
            equipment_key__equipment_id=equipment_id
        )

        queryset = queryset.order_by(
            "-date_key",
            "-hour_key"
        )

        rows = list(
            queryset[:hours]
        )

        # Pour Chart.js :
        # ancien -> récent
        rows.reverse()

        serializer_class = serializers.make_serializer(model)

        serialized = serializer_class(
            rows,
            many=True
        ).data

    else:

        # --------------------------------------------------------
        # Cas "Tous les équipements"
        #
        # Une ligne est produite pour chaque :
        #
        #     (date_key, hour_key)
        #
        # Les équipements sont ensuite agrégés entre eux.
        # --------------------------------------------------------

        field_names = [
            f.name
            for f in model._meta.get_fields()
        ]

        annotations = {}

        # --------------------------------------------------------
        # Moyennes
        # --------------------------------------------------------

        for name in field_names:
            if name.startswith("avg_"):
                annotations[name] = Avg(name)

        # --------------------------------------------------------
        # Minimums
        # --------------------------------------------------------

        for name in field_names:
            if name.startswith("min_"):
                annotations[name] = Min(name)

        # --------------------------------------------------------
        # Maximums
        # --------------------------------------------------------

        for name in field_names:
            if name.startswith("max_"):
                annotations[name] = Max(name)

        # --------------------------------------------------------
        # Compteurs
        # --------------------------------------------------------

        for count_field in (
            "measurement_count",
            "alarm_count",
            "fault_count"
        ):
            if count_field in field_names:
                annotations[count_field] = Sum(count_field)

        # --------------------------------------------------------
        # Regroupement par date + heure
        # --------------------------------------------------------

        grouped = (
            queryset
            .values(
                "date_key",
                "hour_key"
            )
            .annotate(
                **annotations
            )
            .order_by(
                "-date_key",
                "-hour_key"
            )[:hours]
        )

        rows = list(grouped)

        # Pour Chart.js :
        # ancien -> récent
        rows.reverse()

        # Les résultats sont déjà des dictionnaires.
        # Aucun serializer n'est nécessaire ici.
        serialized = rows

    return Response(
        {
            "equipment_type": equipment_type,
            "count": len(serialized),
            "results": serialized,
        }
    )


# ============================================================
# API -- QUALITÉ DES DONNÉES
# ============================================================

@api_view(["GET"])
def rejected_summary(request):
    """
    GET /api/rejected/summary/?hours=24

    Retourne les rejets Spark sur les N dernières heures.

    Les données proviennent de :

        fact_rejected_hourly

    Structure retournée :

        {
            "count": ...,
            "results": [
                {
                    "topic": "...",
                    "reject_reason": "...",
                    "rejected_count": ...
                }
            ]
        }

    Les différentes heures sont regroupées par :
        topic + reject_reason

    Cette structure est adaptée au graphique circulaire
    de qualité des données.
    """

    hours = get_positive_int(
        request,
        "hours",
        default=24,
        minimum=1,
        maximum=24 * 30
    )

    # --------------------------------------------------------
    # Heure actuelle
    # --------------------------------------------------------

    now = timezone.localtime()

    cutoff = now - timedelta(hours=hours)

    cutoff_date_key = (
        cutoff.year * 10000
        + cutoff.month * 100
        + cutoff.day
    )

    current_date_key = (
        now.year * 10000
        + now.month * 100
        + now.day
    )

    cutoff_hour = cutoff.hour
    current_hour = now.hour

    # --------------------------------------------------------
    # Filtre temporel
    #
    # Exemple :
    # maintenant = 26/08 19:00
    # hours = 24
    #
    # => à partir du 25/08 19:00
    # --------------------------------------------------------

    time_filter = (
        Q(
            date_key__gt=cutoff_date_key,
            date_key__lt=current_date_key
        )
        |
        Q(
            date_key=cutoff_date_key,
            hour_key__gte=cutoff_hour
        )
        |
        Q(
            date_key=current_date_key,
            hour_key__lte=current_hour
        )
    )

    queryset = (
        models.FactRejectedHourly
        .objects
        .filter(time_filter)
    )

    # --------------------------------------------------------
    # Agrégation :
    #
    # plusieurs lignes horaires avec le même motif
    # sont regroupées ensemble.
    # --------------------------------------------------------

    summary = (
        queryset
        .values(
            "topic",
            "reject_reason"
        )
        .annotate(
            rejected_count=Sum("rejected_count")
        )
        .order_by(
            "-rejected_count"
        )
    )

    results = list(summary)

    return Response(
        {
            "hours": hours,
            "count": len(results),
            "total_rejected": sum(
                item["rejected_count"] or 0
                for item in results
            ),
            "results": results,
        }
    )


# ============================================================
# API -- REJETS RÉCENTS
# ============================================================

@api_view(["GET"])
def rejected_recent(request):
    """
    GET /api/rejected/recent/?limit=50

    Retourne les dernières données rejetées brutes.

    Contrairement à rejected_summary(), cette vue utilise :

        rejected_data

    et retourne le raw_payload ainsi que le motif du rejet.
    """

    limit = get_positive_int(
        request,
        "limit",
        default=50,
        minimum=1,
        maximum=500
    )

    rows = (
        models.RejectedData
        .objects
        .order_by("-rejected_at")[:limit]
    )

    serialized = serializers.RejectedDataSerializer(
        rows,
        many=True
    ).data

    return Response(
        {
            "count": len(serialized),
            "results": serialized
        }
    )
```

