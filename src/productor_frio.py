import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuración de conexión y topic
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'telemetria-frio'

# Flota de vehículos y coordenadas base (Sevilla y alrededores)
VEHICULOS = {
    'CAM-01': {'lat': 37.3891, 'lon': -5.9845},
    'CAM-02': {'lat': 37.3754, 'lon': -5.9680},
    'CAM-03': {'lat': 37.4020, 'lon': -5.9912},
    'CAM-04': {'lat': 37.3610, 'lon': -5.9750},
}

TOTAL_EVENTOS = 60
TOTAL_ANOMALIAS_OBJETIVO = 8  # Valor fijo dentro del rango estricto [6, 10]


def generar_plan_anomalias(total_eventos: int, num_anomalias: int) -> set[int]:
    """Selecciona aleatoriamente los índices exactos de eventos que serán anómalos."""
    return set(random.sample(range(total_eventos), num_anomalias))


def main():
    # 1. Conexión y fiabilidad: acks="all", clave UTF-8 y valor JSON
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks='all',
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    lista_camiones = list(VEHICULOS.keys())
    indices_anomalos = generar_plan_anomalias(TOTAL_EVENTOS, TOTAL_ANOMALIAS_OBJETIVO)
    contador_anomalias = 0

    print(f"Iniciando simulación de telemetría ({TOTAL_EVENTOS} eventos) hacia '{TOPIC_NAME}'...\n")

    try:
        for i in range(TOTAL_EVENTOS):
            # Selección rotativa para repartir equitativamente eventos y anomalías
            vehiculo_id = lista_camiones[i % len(lista_camiones)]

            # Desplazamiento ligero de GPS simulando movimiento en ruta
            VEHICULOS[vehiculo_id]['lat'] += random.uniform(-0.0015, 0.0015)
            VEHICULOS[vehiculo_id]['lon'] += random.uniform(-0.0015, 0.0015)

            # 5. Inyección controlada de anomalías (temp_c > -18.0)
            es_anomalia = i in indices_anomalos
            if es_anomalia:
                temperatura = round(random.uniform(-17.5, -14.0), 1)
                contador_anomalias += 1
            else:
                temperatura = round(random.uniform(-22.0, -18.5), 1)

            # 3. Estructura JSON obligatoria
            evento = {
                "vehiculo": vehiculo_id,
                "temp_c": temperatura,
                "lat": round(VEHICULOS[vehiculo_id]['lat'], 4),
                "lon": round(VEHICULOS[vehiculo_id]['lon'], 4),
                "timestamp_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            # 4. Envío pasando obligatoriamente 'vehiculo' como key de partición
            futuro = producer.send(
                topic=TOPIC_NAME,
                key=vehiculo_id,
                value=evento,
            )

            # Esperamos confirmación del broker para obtener partición y offset
            record_metadata = futuro.get(timeout=10)

            # 6. Trazabilidad en consola
            marca_alerta = " [ANOMALÍA]" if es_anomalia else ""
            print(
                f"[{i + 1:02d}/{TOTAL_EVENTOS}] "
                f"Vehículo: {vehiculo_id} | "
                f"Temp: {temperatura:5.1f} °C | "
                f"Partición: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}{marca_alerta}"
            )

            # 2. Pausa de 0.5 segundos entre envíos
            time.sleep(0.5)

    except KafkaError as e:
        print(f"\nError de comunicación con Kafka: {e}")
    except KeyboardInterrupt:
        print("\nSimulación interrumpida manualmente por el usuario.")
    finally:
        producer.flush()
        producer.close()
        print("\n" + "=" * 55)
        print(f"Simulación finalizada. Total de anomalías emitidas: {contador_anomalias}")
        print("=" * 55)


if __name__ == '__main__':
    main()