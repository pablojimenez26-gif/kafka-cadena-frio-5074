# Monitorización de Cadena de Frío en Tiempo Real con Apache Kafka (KRaft)

Sistema de ingestión y detección temprana de anomalías térmicas (`temp_c > -18.0 °C`) sobre una flota de transporte refrigerado (`CAM-01` a `CAM-04`), implementado sobre **Apache Kafka 4.0.0** en modo combinado KRaft (sin ZooKeeper) y clientes Python (`kafka-python-ng`).

---

## Estructura del Repositorio

```text
kafka-cadena-frio-5074/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── src/
│   ├── productor_frio.py
│   └── consumidor_alertas.py
└── evidencias/
    ├── E1_topic_describe.txt
    ├── E2_productor_salida.txt
    ├── E3A_group_antes_fallo.txt
    ├── E3B_group_despues_fallo.txt
    └── alertas_detectadas.log
```

---

## Guía de Reproducción del Entorno

### 1. Requisitos previos
* **Docker Desktop** (o Docker Engine + Docker Compose v2) operativo.
* **Python 3.10+** instalado en el sistema anfitrión.

### 2. Despliegue de Kafka (KRaft) y creación del Topic
1. Levanta el contenedor de Kafka en segundo plano:
   ```bash
   docker compose up -d
   ```
2. Verifica que esté activo (`docker compose ps`) y crea el topic `telemetria-frio` (3 particiones, factor de replicación 1):
   ```bash
   docker exec kafka-kraft /opt/kafka/bin/kafka-topics.sh \
     --create --topic telemetria-frio \
     --partitions 3 --replication-factor 1 \
     --bootstrap-server localhost:9092
   ```

### 3. Configuración del entorno Python (`venv`)
En la raíz del proyecto, crea el entorno virtual, actívalo e instala las dependencias:

* **Windows (PowerShell):**
  ```powershell
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  pip install -r requirements.txt
  ```
* **Linux / macOS:**
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ```

### 4. Ejecución de la simulación y prueba de failover
1. Abre **dos terminales** (Consumidor A y Consumidor B), activa `.venv` en ambas y arranca el consumidor:
   ```bash
   python src/consumidor_alertas.py
   ```
2. En una **tercera terminal**, lanza el productor (60 eventos con 8 anomalías inyectadas):
   ```bash
   python src/productor_frio.py
   ```
3. Hacia el mensaje 25–30, detén uno de los consumidores con `Ctrl + C`. El consumidor superviviente asumirá las 3 particiones y guardará todas las alertas en `evidencias/alertas_detectadas.log` con `LAG = 0` final.

---

## Análisis Técnico

### 1. Particionado por clave (`key=vehiculo`) y ordenamiento
Al enviar el identificador del camión (`CAM-01`..`CAM-04`) como `key`, Kafka aplica un hash determinista (*murmur2*) que enruta todos los mensajes de un mismo vehículo siempre a la **misma partición**. Dado que Kafka solo garantiza el orden estricto dentro de cada partición individual, esto asegura reconstruir la serie temporal de cada camión sin desorden y reparte equilibradamente las 4 claves entre las 3 particiones.

### 2. Garantías de entrega y semántica *At-Least-Once*
* **Productor (`acks="all"`):** Exige confirmación de todas las réplicas sincronizadas (ISR) antes de dar el envío por válido, evitando pérdida de datos ante caídas del broker.
* **Consumidor (`enable_auto_commit=False` + `commit()` manual):** Sigue el flujo estricto **Procesar → Escribir y volcar a disco (`flush()`) → Confirmar (`commit()`)**. Si el proceso cae antes del `commit()`, Kafka reenviará el mensaje desde el último offset guardado, garantizando **cero pérdida de alertas** (*at-least-once*).

### 3. Escalabilidad y tolerancia a fallos en Consumer Groups
Al compartir el `group_id` (`grupo-alertas-frio`), Kafka reparte las 3 particiones en exclusiva entre las instancias activas (2 + 1). Si un consumidor cae (`Ctrl + C`), deja de enviar *heartbeats* y, tras expirar el *session timeout*, el *Group Coordinator* ejecuta un **rebalanceo**: reasigna la partición huérfana al consumidor superviviente desde el último offset confirmado hasta dejar el `LAG` en `0`.