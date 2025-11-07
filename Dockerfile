FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos la carpeta completa de ETL
COPY ETL /app/ETL

# Desactivar buffering de stdout/stderr para que los print() se muestren en tiempo real
ENV PYTHONUNBUFFERED=1

# Creamos carpeta de datos
RUN mkdir -p /app/data/logs

CMD ["python", "ETL/script/run_all.py"]
