#Imagen base con python
FROM python:3.12-slim

#Crear un directorio de trabajo dentro del contenedor
WORKDIR /app
#Copiar los archivos de requerimientos al contenedor
COPY ./ETL/script/pib_ine_extract.py /app/
COPY ./requirements.txt /app/

#Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Crear  carpeta de datos (volumen)
RUN mkdir -p /app/data/pib_ine_extract

#Comando por defecto al iniciar el contenedor
CMD ["python", "pib_ine_extract.py"]