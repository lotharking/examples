FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema y Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Configurar Java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Copiar todos los archivos
COPY . /app/

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Ejecutar la comparación
CMD ["python", "comparacion_spark_pandas.py"]