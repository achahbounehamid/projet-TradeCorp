# 1. Image de base 
FROM jupyter/pyspark-notebook:latest

# 2. Copie du fichier de dépendances
COPY requirements.txt /tmp/requirements.txt

# 3. Installation des dépendances
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 4. Copie du code source dans le conteneur
COPY . /home/jovyan
WORKDIR /home/jovyan