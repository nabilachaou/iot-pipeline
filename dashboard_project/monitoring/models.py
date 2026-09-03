
from django.db import models


# ============================================================
# COUCHE OPÉRATIONNELLE
# ============================================================
# Tables :
#   elevator_data_clean
#   rmg_data_clean
#   rtg_data_clean
#   sts_data_clean
#   straddle_data_clean
#   tractor_data_clean
#   rejected_data
# ============================================================


class ElevatorDataClean(models.Model):
    id = models.BigIntegerField(primary_key=True)

    equipment_id = models.CharField(max_length=50)
    event_timestamp = models.DateTimeField()

    type = models.CharField(max_length=20, null=True)
    state = models.CharField(max_length=20, null=True)
    position = models.CharField(max_length=20, null=True)

    speed = models.FloatField(null=True)
    load_value = models.FloatField(null=True)

    direction = models.CharField(max_length=20, null=True)

    temperature = models.FloatField(null=True)
    energy = models.FloatField(null=True)
    engine_hours = models.FloatField(null=True)

    alarm = models.CharField(max_length=30, null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "elevator_data_clean"
        ordering = ["-event_timestamp"]


class RmgDataClean(models.Model):
    id = models.BigIntegerField(primary_key=True)

    equipment_id = models.CharField(max_length=50)
    event_timestamp = models.DateTimeField()

    type = models.CharField(max_length=20, null=True)
    state = models.CharField(max_length=20, null=True)

    rail_position = models.FloatField(null=True)
    trolley_position = models.FloatField(null=True)
    hoist_height = models.FloatField(null=True)
    travelling_speed = models.FloatField(null=True)
    load_weight = models.FloatField(null=True)
    motor_current = models.FloatField(null=True)
    vibration = models.FloatField(null=True)
    temperature = models.FloatField(null=True)
    energy = models.FloatField(null=True)
    engine_hours = models.FloatField(null=True)

    alarm = models.CharField(max_length=30, null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "rmg_data_clean"
        ordering = ["-event_timestamp"]


class RtgDataClean(models.Model):
    id = models.BigIntegerField(primary_key=True)

    equipment_id = models.CharField(max_length=50)
    event_timestamp = models.DateTimeField()

    type = models.CharField(max_length=20, null=True)
    state = models.CharField(max_length=20, null=True)

    trolley_position = models.FloatField(null=True)
    hoist_height = models.FloatField(null=True)
    gantry_speed = models.FloatField(null=True)
    load_weight = models.FloatField(null=True)
    hydraulic_pressure = models.FloatField(null=True)
    vibration = models.FloatField(null=True)
    temperature = models.FloatField(null=True)
    energy = models.FloatField(null=True)
    engine_hours = models.FloatField(null=True)

    alarm = models.CharField(max_length=30, null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "rtg_data_clean"
        ordering = ["-event_timestamp"]


class StsDataClean(models.Model):
    id = models.BigIntegerField(primary_key=True)

    equipment_id = models.CharField(max_length=50)
    event_timestamp = models.DateTimeField()

    type = models.CharField(max_length=20, null=True)
    state = models.CharField(max_length=20, null=True)

    motor_temperature = models.FloatField(null=True)
    vibration = models.FloatField(null=True)
    container_load_kg = models.FloatField(null=True)
    trolley_position_m = models.FloatField(null=True)
    spreader_height_m = models.FloatField(null=True)
    movement_speed_m_s = models.FloatField(null=True)
    energy_consumption_kw = models.FloatField(null=True)
    hydraulic_oil_level = models.FloatField(null=True)

    status = models.CharField(max_length=30, null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "sts_data_clean"
        ordering = ["-event_timestamp"]


class StraddleDataClean(models.Model):
    id = models.BigIntegerField(primary_key=True)

    equipment_id = models.CharField(max_length=50)
    event_timestamp = models.DateTimeField()

    type = models.CharField(max_length=20, null=True)
    state = models.CharField(max_length=20, null=True)

    position_x = models.FloatField(null=True)
    position_y = models.FloatField(null=True)
    speed = models.FloatField(null=True)
    container_load = models.FloatField(null=True)
    height = models.FloatField(null=True)

    operation_mode = models.CharField(max_length=20, null=True)

    temperature = models.FloatField(null=True)
    energy = models.FloatField(null=True)
    engine_hours = models.FloatField(null=True)

    alarm = models.CharField(max_length=30, null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "straddle_data_clean"
        ordering = ["-event_timestamp"]


class TractorDataClean(models.Model):
    id = models.BigIntegerField(primary_key=True)

    equipment_id = models.CharField(max_length=50)
    event_timestamp = models.DateTimeField()

    type = models.CharField(max_length=20, null=True)
    state = models.CharField(max_length=20, null=True)

    position_x = models.FloatField(null=True)
    position_y = models.FloatField(null=True)
    speed = models.FloatField(null=True)
    fuel_level = models.FloatField(null=True)

    container_attached = models.BooleanField(null=True)

    operation_mode = models.CharField(max_length=20, null=True)

    temperature = models.FloatField(null=True)
    energy = models.FloatField(null=True)
    engine_hours = models.FloatField(null=True)

    alarm = models.CharField(max_length=30, null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "tractor_data_clean"
        ordering = ["-event_timestamp"]


class RejectedData(models.Model):
    id = models.BigIntegerField(primary_key=True)

    topic = models.CharField(max_length=50, null=True)
    equipment_id = models.CharField(max_length=50, null=True)

    raw_payload = models.TextField(null=True)

    reject_reason = models.CharField(max_length=50, null=True)

    rejected_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "rejected_data"
        ordering = ["-rejected_at"]


# ============================================================
# DIMENSIONS
# ============================================================


class DimEquipment(models.Model):
    equipment_key = models.BigIntegerField(primary_key=True)

    equipment_id = models.CharField(max_length=50)
    equipment_type = models.CharField(max_length=20)

    created_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "dim_equipment"


class DimDate(models.Model):
    date_key = models.IntegerField(primary_key=True)

    full_date = models.DateField()

    year = models.IntegerField()
    month = models.IntegerField()
    day = models.IntegerField()

    day_of_week = models.IntegerField()
    week_of_year = models.IntegerField()

    month_name = models.CharField(max_length=20)

    is_weekend = models.BooleanField()

    class Meta:
        managed = False
        db_table = "dim_date"


class DimHour(models.Model):
    hour_key = models.IntegerField(primary_key=True)

    hour_label = models.CharField(max_length=5)

    class Meta:
        managed = False
        db_table = "dim_hour"


# ============================================================
# FACT ELEVATOR
# ============================================================


class FactElevatorHourly(models.Model):
    fact_key = models.BigIntegerField(primary_key=True)

    equipment_key = models.ForeignKey(
        DimEquipment,
        on_delete=models.DO_NOTHING,
        db_column="equipment_key",
    )

    date_key = models.IntegerField()
    hour_key = models.IntegerField()

    measurement_count = models.IntegerField()
    alarm_count = models.IntegerField()

    avg_speed = models.FloatField(null=True)
    min_speed = models.FloatField(null=True)
    max_speed = models.FloatField(null=True)

    avg_load_value = models.FloatField(null=True)
    min_load_value = models.FloatField(null=True)
    max_load_value = models.FloatField(null=True)

    avg_temperature = models.FloatField(null=True)
    min_temperature = models.FloatField(null=True)
    max_temperature = models.FloatField(null=True)

    avg_energy = models.FloatField(null=True)
    min_energy = models.FloatField(null=True)
    max_energy = models.FloatField(null=True)

    max_engine_hours = models.FloatField(null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fact_elevator_hourly"
        ordering = ["-date_key", "-hour_key"]


# ============================================================
# FACT RMG
# ============================================================


class FactRmgHourly(models.Model):
    fact_key = models.BigIntegerField(primary_key=True)

    equipment_key = models.ForeignKey(
        DimEquipment,
        on_delete=models.DO_NOTHING,
        db_column="equipment_key",
    )

    date_key = models.IntegerField()
    hour_key = models.IntegerField()

    measurement_count = models.IntegerField()
    alarm_count = models.IntegerField()

    avg_rail_position = models.FloatField(null=True)
    min_rail_position = models.FloatField(null=True)
    max_rail_position = models.FloatField(null=True)

    avg_trolley_position = models.FloatField(null=True)
    min_trolley_position = models.FloatField(null=True)
    max_trolley_position = models.FloatField(null=True)

    avg_hoist_height = models.FloatField(null=True)
    min_hoist_height = models.FloatField(null=True)
    max_hoist_height = models.FloatField(null=True)

    avg_travelling_speed = models.FloatField(null=True)
    min_travelling_speed = models.FloatField(null=True)
    max_travelling_speed = models.FloatField(null=True)

    avg_load_weight = models.FloatField(null=True)
    min_load_weight = models.FloatField(null=True)
    max_load_weight = models.FloatField(null=True)

    avg_motor_current = models.FloatField(null=True)
    min_motor_current = models.FloatField(null=True)
    max_motor_current = models.FloatField(null=True)

    avg_vibration = models.FloatField(null=True)
    min_vibration = models.FloatField(null=True)
    max_vibration = models.FloatField(null=True)

    avg_temperature = models.FloatField(null=True)
    min_temperature = models.FloatField(null=True)
    max_temperature = models.FloatField(null=True)

    avg_energy = models.FloatField(null=True)
    min_energy = models.FloatField(null=True)
    max_energy = models.FloatField(null=True)

    max_engine_hours = models.FloatField(null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fact_rmg_hourly"
        ordering = ["-date_key", "-hour_key"]


# ============================================================
# FACT RTG
# ============================================================


class FactRtgHourly(models.Model):
    fact_key = models.BigIntegerField(primary_key=True)

    equipment_key = models.ForeignKey(
        DimEquipment,
        on_delete=models.DO_NOTHING,
        db_column="equipment_key",
    )

    date_key = models.IntegerField()
    hour_key = models.IntegerField()

    measurement_count = models.IntegerField()
    alarm_count = models.IntegerField()

    avg_trolley_position = models.FloatField(null=True)
    min_trolley_position = models.FloatField(null=True)
    max_trolley_position = models.FloatField(null=True)

    avg_hoist_height = models.FloatField(null=True)
    min_hoist_height = models.FloatField(null=True)
    max_hoist_height = models.FloatField(null=True)

    avg_gantry_speed = models.FloatField(null=True)
    min_gantry_speed = models.FloatField(null=True)
    max_gantry_speed = models.FloatField(null=True)

    avg_load_weight = models.FloatField(null=True)
    min_load_weight = models.FloatField(null=True)
    max_load_weight = models.FloatField(null=True)

    avg_hydraulic_pressure = models.FloatField(null=True)
    min_hydraulic_pressure = models.FloatField(null=True)
    max_hydraulic_pressure = models.FloatField(null=True)

    avg_vibration = models.FloatField(null=True)
    min_vibration = models.FloatField(null=True)
    max_vibration = models.FloatField(null=True)

    avg_temperature = models.FloatField(null=True)
    min_temperature = models.FloatField(null=True)
    max_temperature = models.FloatField(null=True)

    avg_energy = models.FloatField(null=True)
    min_energy = models.FloatField(null=True)
    max_energy = models.FloatField(null=True)

    max_engine_hours = models.FloatField(null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fact_rtg_hourly"
        ordering = ["-date_key", "-hour_key"]


# ============================================================
# FACT STS
# ============================================================


class FactStsHourly(models.Model):
    fact_key = models.BigIntegerField(primary_key=True)

    equipment_key = models.ForeignKey(
        DimEquipment,
        on_delete=models.DO_NOTHING,
        db_column="equipment_key",
    )

    date_key = models.IntegerField()
    hour_key = models.IntegerField()

    measurement_count = models.IntegerField()
    fault_count = models.IntegerField()

    avg_motor_temperature = models.FloatField(null=True)
    min_motor_temperature = models.FloatField(null=True)
    max_motor_temperature = models.FloatField(null=True)

    avg_vibration = models.FloatField(null=True)
    min_vibration = models.FloatField(null=True)
    max_vibration = models.FloatField(null=True)

    avg_container_load_kg = models.FloatField(null=True)
    min_container_load_kg = models.FloatField(null=True)
    max_container_load_kg = models.FloatField(null=True)

    avg_trolley_position_m = models.FloatField(null=True)
    min_trolley_position_m = models.FloatField(null=True)
    max_trolley_position_m = models.FloatField(null=True)

    avg_spreader_height_m = models.FloatField(null=True)
    min_spreader_height_m = models.FloatField(null=True)
    max_spreader_height_m = models.FloatField(null=True)

    avg_movement_speed_m_s = models.FloatField(null=True)
    min_movement_speed_m_s = models.FloatField(null=True)
    max_movement_speed_m_s = models.FloatField(null=True)

    avg_energy_consumption_kw = models.FloatField(null=True)
    min_energy_consumption_kw = models.FloatField(null=True)
    max_energy_consumption_kw = models.FloatField(null=True)

    avg_hydraulic_oil_level = models.FloatField(null=True)
    min_hydraulic_oil_level = models.FloatField(null=True)
    max_hydraulic_oil_level = models.FloatField(null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fact_sts_hourly"
        ordering = ["-date_key", "-hour_key"]


# ============================================================
# FACT STRADDLE
# ============================================================


class FactStraddleHourly(models.Model):
    fact_key = models.BigIntegerField(primary_key=True)

    equipment_key = models.ForeignKey(
        DimEquipment,
        on_delete=models.DO_NOTHING,
        db_column="equipment_key",
    )

    date_key = models.IntegerField()
    hour_key = models.IntegerField()

    measurement_count = models.IntegerField()
    alarm_count = models.IntegerField()

    avg_position_x = models.FloatField(null=True)
    min_position_x = models.FloatField(null=True)
    max_position_x = models.FloatField(null=True)

    avg_position_y = models.FloatField(null=True)
    min_position_y = models.FloatField(null=True)
    max_position_y = models.FloatField(null=True)

    avg_speed = models.FloatField(null=True)
    min_speed = models.FloatField(null=True)
    max_speed = models.FloatField(null=True)

    avg_container_load = models.FloatField(null=True)
    min_container_load = models.FloatField(null=True)
    max_container_load = models.FloatField(null=True)

    avg_height = models.FloatField(null=True)
    min_height = models.FloatField(null=True)
    max_height = models.FloatField(null=True)

    avg_temperature = models.FloatField(null=True)
    min_temperature = models.FloatField(null=True)
    max_temperature = models.FloatField(null=True)

    avg_energy = models.FloatField(null=True)
    min_energy = models.FloatField(null=True)
    max_energy = models.FloatField(null=True)

    max_engine_hours = models.FloatField(null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fact_straddle_hourly"
        ordering = ["-date_key", "-hour_key"]


# ============================================================
# FACT TRACTOR
# ============================================================


class FactTractorHourly(models.Model):
    fact_key = models.BigIntegerField(primary_key=True)

    equipment_key = models.ForeignKey(
        DimEquipment,
        on_delete=models.DO_NOTHING,
        db_column="equipment_key",
    )

    date_key = models.IntegerField()
    hour_key = models.IntegerField()

    measurement_count = models.IntegerField()
    alarm_count = models.IntegerField()

    avg_position_x = models.FloatField(null=True)
    min_position_x = models.FloatField(null=True)
    max_position_x = models.FloatField(null=True)

    avg_position_y = models.FloatField(null=True)
    min_position_y = models.FloatField(null=True)
    max_position_y = models.FloatField(null=True)

    avg_speed = models.FloatField(null=True)
    min_speed = models.FloatField(null=True)
    max_speed = models.FloatField(null=True)

    avg_fuel_level = models.FloatField(null=True)
    min_fuel_level = models.FloatField(null=True)
    max_fuel_level = models.FloatField(null=True)

    avg_temperature = models.FloatField(null=True)
    min_temperature = models.FloatField(null=True)
    max_temperature = models.FloatField(null=True)

    avg_energy = models.FloatField(null=True)
    min_energy = models.FloatField(null=True)
    max_energy = models.FloatField(null=True)

    max_engine_hours = models.FloatField(null=True)

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fact_tractor_hourly"
        ordering = ["-date_key", "-hour_key"]


# ============================================================
# FACT REJECTED
# ============================================================


class FactRejectedHourly(models.Model):
    fact_key = models.BigIntegerField(primary_key=True)

    date_key = models.IntegerField()
    hour_key = models.IntegerField()

    topic = models.CharField(max_length=50)
    reject_reason = models.CharField(max_length=50)

    rejected_count = models.IntegerField()

    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fact_rejected_hourly"
        ordering = ["-date_key", "-hour_key"]
