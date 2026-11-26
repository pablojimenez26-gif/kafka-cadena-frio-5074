import json
import os
from pathlib import Path
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configuración de conexión, topic y grupo de consumidores
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'telemetria-frio'
GROUP_ID = 'grupo-alertas-frio'

# Umbral crítico de cadena de frío y ruta del fichero de evidencias
UMBRAL_TEMP_C = -18.0
RUTA_LOG = Path('evidencias') / 'alertas_detectadas.log'


def main():
    # Asegura que el directorio 'evidencias/' exista antes de abrir el archivo
    RUTA_LOG.parent.mkdir(parents=True, exist_ok=True)

    # Suscripción al topic con offsets manuales y lectura desde el principio
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode('utf-8') if k is not None else None,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )

    print(f"Consumidor iniciado en el grupo '{GROUP_ID}' escuchando '{TOPIC_NAME}'...")
    print(f"Registrando anomalías (temp_c > {UMBRAL_TEMP_C} °C) en: {RUTA_LOG}")
    print("Pulsa Ctrl+C para detener el consumidor.\n")

    # Apertura del fichero de evidencias en modo append ('a')
    with open(RUTA_LOG, mode='a', encoding='utf-8') as log_file:
        try:
            for mensaje in consumer:
                datos = mensaje.value
                particion = mensaje.partition
                offset = mensaje.offset
                clave = mensaje.key
                temp_c = datos['temp_c']

                # 1. Trazabilidad estándar por consola de cada mensaje recibido
                print(
                    f"[Partición: {particion} | Offset: {offset}] "
                    f"Vehículo: {clave} | Temp: {temp_c:5.1f} °C"
                )

                # 2. Detección de anomalía térmica (temp_c > -18.0)
                if temp_c > UMBRAL_TEMP_C:
                    print(
                        f"  >>> [ALERTA DE CADENA DE FRÍO] Vehículo {clave} "
                        f"superó el umbral con {temp_c:.1f} °C "
                        f"(Partición {particion}, Offset {offset}) <<<"
                    )

                    linea_log = (
                        f"timestamp_iso={datos.get('timestamp_iso')} | "
                        f"vehiculo={clave} | "
                        f"temp_c={temp_c} | "
                        f"lat={datos.get('lat')} | "
                        f"lon={datos.get('lon')} | "
                        f"particion={particion} | "
                        f"offset={offset}\n"
                    )
                    log_file.write(linea_log)
                    log_file.flush()

                # 3. Confirmación explícita del offset tras procesar y persistir
                consumer.commit()

        except KeyboardInterrupt:
            print("\nDeteniendo consumidor de alertas...")
        except KafkaError as e:
            print(f"\nError en el consumidor de Kafka: {e}")
        finally:
            consumer.close()
            print("Consumidor cerrado correctamente.")


if __name__ == '__main__':
    main()