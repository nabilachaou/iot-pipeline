

from . import models

EQUIPMENT_REGISTRY = {
    "elevator": {
        "label": "Elevator",
        "topic": "elevator_data",
        "clean_model": models.ElevatorDataClean,
        "fact_model": models.FactElevatorHourly,
        "numeric_fields": ["speed", "load_value", "temperature", "energy", "engine_hours"],
        "status_field": "alarm",
    },
    "rmg": {
        "label": "Rail Mounted Gantry (RMG)",
        "topic": "rmg_data",
        "clean_model": models.RmgDataClean,
        "fact_model": models.FactRmgHourly,
        "numeric_fields": [
            "rail_position", "trolley_position", "hoist_height",
            "travelling_speed", "load_weight", "motor_current",
            "vibration", "temperature", "energy", "engine_hours",
        ],
        "status_field": "alarm",
    },
    "rtg": {
        "label": "Rubber Tyred Gantry (RTG)",
        "topic": "rtg_data",
        "clean_model": models.RtgDataClean,
        "fact_model": models.FactRtgHourly,
        "numeric_fields": [
            "trolley_position", "hoist_height", "gantry_speed",
            "load_weight", "hydraulic_pressure", "vibration",
            "temperature", "energy", "engine_hours",
        ],
        "status_field": "alarm",
    },
    "sts": {
        "label": "Ship-to-Shore Crane (STS)",
        "topic": "sts_data",
        "clean_model": models.StsDataClean,
        "fact_model": models.FactStsHourly,
        "numeric_fields": [
            "motor_temperature", "vibration", "container_load_kg",
            "trolley_position_m", "spreader_height_m",
            "movement_speed_m_s", "energy_consumption_kw",
            "hydraulic_oil_level",
        ],
        "status_field": "status",
    },
    "straddle": {
        "label": "Straddle Carrier",
        "topic": "straddle_data",
        "clean_model": models.StraddleDataClean,
        "fact_model": models.FactStraddleHourly,
        "numeric_fields": [
            "position_x", "position_y", "speed", "container_load",
            "height", "temperature", "energy", "engine_hours",
        ],
        "status_field": "alarm",
    },
    "tractor": {
        "label": "Terminal Tractor",
        "topic": "tractor_data",
        "clean_model": models.TractorDataClean,
        "fact_model": models.FactTractorHourly,
        "numeric_fields": [
            "position_x", "position_y", "speed", "fuel_level",
            "temperature", "energy", "engine_hours",
        ],
        "status_field": "alarm",
    },
}


def get_equipment_config(equipment_type):
    """
    Retourne la configuration associée à un type d'équipement, ou None si
    le type demandé n'existe pas (permet aux vues de répondre 404 proprement
    plutôt que de lever une KeyError).
    """
    return EQUIPMENT_REGISTRY.get(equipment_type)
