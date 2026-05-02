from pathlib import Path
import zipfile
import xml.etree.ElementTree as ET
import csv
import pandas as pd


# =========================
# 1. CONFIGURACIÓN '/Users/lm/Library/Mobile Documents/com~apple~CloudDocs/exportación.zip'
# =========================

RUTA_ENTRADA = Path("/Users/lm/Library/Mobile Documents/com~apple~CloudDocs/exportación.zip")
# También puedes poner directamente:
# RUTA_ENTRADA = Path(r"C:\Users\TU_USUARIO\Downloads\export.xml")

CARPETA_SALIDA = Path("/Users/lm/Library/Mobile Documents/com~apple~CloudDocs/apple_health_tablas")

ZONA_HORARIA = "Europe/Madrid"


TIPOS_INTERES = {
    "HKQuantityTypeIdentifierStepCount",
    "HKQuantityTypeIdentifierActiveEnergyBurned",
    "HKQuantityTypeIdentifierDistanceWalkingRunning",
    "HKQuantityTypeIdentifierAppleExerciseTime",
    "HKQuantityTypeIdentifierHeartRate",
    "HKQuantityTypeIdentifierRestingHeartRate",
    "HKQuantityTypeIdentifierWalkingHeartRateAverage",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN",
    "HKQuantityTypeIdentifierBodyMass",
    "HKQuantityTypeIdentifierBodyFatPercentage",
    "HKQuantityTypeIdentifierVO2Max",
    "HKCategoryTypeIdentifierSleepAnalysis",
}


# =========================
# 2. FUNCIONES AUXILIARES
# =========================

def limpiar_tipo(tipo):
    return (
        tipo.replace("HKQuantityTypeIdentifier", "")
            .replace("HKCategoryTypeIdentifier", "")
    )


def abrir_export_xml(ruta):
    if ruta.suffix.lower() == ".zip":
        zip_file = zipfile.ZipFile(ruta)

        posibles = [
            info for info in zip_file.infolist()
            if info.filename.lower().endswith(".xml")
            and "apple_health_export/" in info.filename
        ]

        if not posibles:
            raise FileNotFoundError("No se encontró ningún XML de Apple Health dentro del ZIP.")

        # Elegimos el XML más grande, que normalmente es el archivo principal de datos
        archivo_principal = max(posibles, key=lambda x: x.file_size)

        print(f"XML encontrado: {archivo_principal.filename}")
        print(f"Tamaño aproximado: {archivo_principal.file_size / 1024 / 1024:.1f} MB")

        return zip_file.open(archivo_principal.filename), zip_file

    return open(ruta, "rb"), None


def convertir_fecha_apple(serie):
    fecha = pd.to_datetime(serie, errors="coerce", utc=True)
    fecha = fecha.dt.tz_convert(ZONA_HORARIA)
    return fecha.dt.tz_localize(None)


def convertir_a_numero(serie):
    return pd.to_numeric(serie, errors="coerce")


# =========================
# 3. EXTRAER XML A CSV BASE
# =========================

def extraer_xml_a_csv():
    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

    archivo_records = CARPETA_SALIDA / "00_records_base.csv"
    archivo_workouts = CARPETA_SALIDA / "00_entrenamientos_base.csv"

    campos_records = [
        "type",
        "type_clean",
        "sourceName",
        "unit",
        "creationDate",
        "startDate",
        "endDate",
        "value",
    ]

    campos_workouts = [
        "workoutActivityType",
        "sourceName",
        "creationDate",
        "startDate",
        "endDate",
        "duration",
        "durationUnit",
        "totalDistance",
        "totalDistanceUnit",
        "totalEnergyBurned",
        "totalEnergyBurnedUnit",
    ]

    xml_file, zip_file = abrir_export_xml(RUTA_ENTRADA)

    with xml_file:
        with open(archivo_records, "w", newline="", encoding="utf-8-sig") as f_records, \
             open(archivo_workouts, "w", newline="", encoding="utf-8-sig") as f_workouts:

            writer_records = csv.DictWriter(f_records, fieldnames=campos_records)
            writer_workouts = csv.DictWriter(f_workouts, fieldnames=campos_workouts)

            writer_records.writeheader()
            writer_workouts.writeheader()

            for event, elem in ET.iterparse(xml_file, events=("end",)):
                if elem.tag == "Record":
                    tipo = elem.attrib.get("type")

                    if tipo in TIPOS_INTERES:
                        fila = {
                            "type": tipo,
                            "type_clean": limpiar_tipo(tipo),
                            "sourceName": elem.attrib.get("sourceName"),
                            "unit": elem.attrib.get("unit"),
                            "creationDate": elem.attrib.get("creationDate"),
                            "startDate": elem.attrib.get("startDate"),
                            "endDate": elem.attrib.get("endDate"),
                            "value": elem.attrib.get("value"),
                        }
                        writer_records.writerow(fila)

                elif elem.tag == "Workout":
                    fila = {campo: elem.attrib.get(campo) for campo in campos_workouts}
                    writer_workouts.writerow(fila)

                elem.clear()

    if zip_file:
        zip_file.close()

    print("CSV base creados correctamente.")


# =========================
# 4. CREAR TABLAS LIMPIAS
# =========================

def crear_tablas_limpias():
    records_path = CARPETA_SALIDA / "00_records_base.csv"
    workouts_path = CARPETA_SALIDA / "00_entrenamientos_base.csv"

    df = pd.read_csv(records_path, dtype=str)

    for col in ["creationDate", "startDate", "endDate"]:
        df[col] = convertir_fecha_apple(df[col])

    df["value_num"] = convertir_a_numero(df["value"])
    df["fecha_inicio"] = df["startDate"].dt.date
    df["fecha_fin"] = df["endDate"].dt.date

    # -------------------------
    # Actividad diaria
    # -------------------------

    mapa_actividad = {
        "StepCount": "pasos",
        "ActiveEnergyBurned": "energia_activa",
        "DistanceWalkingRunning": "distancia_andando_corriendo",
        "AppleExerciseTime": "minutos_ejercicio",
    }

    actividad = df[df["type_clean"].isin(mapa_actividad.keys())].copy()
    actividad["metrica"] = actividad["type_clean"].map(mapa_actividad)

    actividad_diaria = (
        actividad
        .groupby(["fecha_inicio", "metrica"], as_index=False)["value_num"]
        .sum()
        .pivot(index="fecha_inicio", columns="metrica", values="value_num")
        .reset_index()
        .rename(columns={"fecha_inicio": "fecha"})
    )

    actividad_diaria.to_csv(
        CARPETA_SALIDA / "01_actividad_diaria.csv",
        index=False,
        encoding="utf-8-sig"
    )

    # -------------------------
    # Peso y composición corporal
    # -------------------------

    peso = df[df["type_clean"].isin(["BodyMass", "BodyFatPercentage"])].copy()

    peso = (
        peso
        .sort_values("startDate")
        .groupby(["fecha_inicio", "type_clean"], as_index=False)
        .tail(1)
    )

    peso_tabla = (
        peso
        .pivot(index="fecha_inicio", columns="type_clean", values="value_num")
        .reset_index()
        .rename(columns={
            "fecha_inicio": "fecha",
            "BodyMass": "peso_kg",
            "BodyFatPercentage": "grasa_corporal_pct",
        })
    )

    peso_tabla.to_csv(
        CARPETA_SALIDA / "02_peso_composicion.csv",
        index=False,
        encoding="utf-8-sig"
    )

    # -------------------------
    # Frecuencia cardiaca diaria
    # -------------------------

    fc = df[df["type_clean"] == "HeartRate"].copy()

    fc_diaria = (
        fc
        .groupby("fecha_inicio")
        .agg(
            fc_media=("value_num", "mean"),
            fc_min=("value_num", "min"),
            fc_max=("value_num", "max"),
            lecturas=("value_num", "count")
        )
        .reset_index()
        .rename(columns={"fecha_inicio": "fecha"})
    )

    reposo = df[df["type_clean"] == "RestingHeartRate"].copy()

    fc_reposo = (
        reposo
        .groupby("fecha_inicio")
        .agg(fc_reposo=("value_num", "mean"))
        .reset_index()
        .rename(columns={"fecha_inicio": "fecha"})
    )

    fc_diaria = fc_diaria.merge(fc_reposo, on="fecha", how="outer")

    fc_diaria.to_csv(
        CARPETA_SALIDA / "03_frecuencia_cardiaca.csv",
        index=False,
        encoding="utf-8-sig"
    )

    # -------------------------
    # Sueño
    # -------------------------

    sueno = df[df["type_clean"] == "SleepAnalysis"].copy()

    sueno["duracion_min"] = (
        sueno["endDate"] - sueno["startDate"]
    ).dt.total_seconds() / 60

    # Para sueño usamos la fecha de despertar
    sueno["fecha_sueno"] = sueno["endDate"].dt.date

    sueno["fase_sueno"] = (
        sueno["value"]
        .str.replace("HKCategoryValueSleepAnalysis", "", regex=False)
    )

    sueno_fases = (
        sueno
        .groupby(["fecha_sueno", "fase_sueno"], as_index=False)["duracion_min"]
        .sum()
        .pivot(index="fecha_sueno", columns="fase_sueno", values="duracion_min")
        .reset_index()
        .rename(columns={"fecha_sueno": "fecha"})
    )

    sueno_fases.to_csv(
        CARPETA_SALIDA / "04_sueno_por_fases.csv",
        index=False,
        encoding="utf-8-sig"
    )

    # -------------------------
    # Entrenamientos
    # -------------------------

    workouts = pd.read_csv(workouts_path, dtype=str)

    if not workouts.empty:
        for col in ["creationDate", "startDate", "endDate"]:
            workouts[col] = convertir_fecha_apple(workouts[col])

        workouts["duration"] = convertir_a_numero(workouts["duration"])
        workouts["totalDistance"] = convertir_a_numero(workouts["totalDistance"])
        workouts["totalEnergyBurned"] = convertir_a_numero(workouts["totalEnergyBurned"])
        workouts["fecha"] = workouts["startDate"].dt.date

        workouts.to_csv(
            CARPETA_SALIDA / "05_entrenamientos.csv",
            index=False,
            encoding="utf-8-sig"
        )

        entrenos_diarios = (
            workouts
            .groupby("fecha")
            .agg(
                entrenamientos=("workoutActivityType", "count"),
                duracion_total=("duration", "sum"),
                energia_total=("totalEnergyBurned", "sum"),
                distancia_total=("totalDistance", "sum")
            )
            .reset_index()
        )

        entrenos_diarios.to_csv(
            CARPETA_SALIDA / "06_entrenamientos_diarios.csv",
            index=False,
            encoding="utf-8-sig"
        )

    print("Tablas limpias creadas correctamente.")


# =========================
# 5. EJECUCIÓN
# =========================

if __name__ == "__main__":
    extraer_xml_a_csv()
    crear_tablas_limpias()
    print(f"Proceso terminado. Archivos creados en: {CARPETA_SALIDA}")
