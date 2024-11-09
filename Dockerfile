# Dockerfile

# Primera etapa: Construcción
FROM python:3.11-slim AS builder

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el archivo de dependencias
COPY requirements.txt .

# Instalar las dependencias en un directorio separado
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Segunda etapa: Ejecución
FROM python:3.11-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar las dependencias desde la etapa de construcción
COPY --from=builder /install /usr/local

# Copiar el resto del proyecto
COPY . .

# Comando por defecto para ejecutar el script principal
CMD ["python", "src/pipeline.py"]
