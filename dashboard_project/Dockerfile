# Dockerfile pour le tableau de bord Django (visualisation en lecture seule
# du pipeline IoT). Ce service ne modifie jamais les tables du pipeline :
# il se contente de les lire via des modèles Django déclarés managed=False
# (cf. monitoring/models.py).

FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# collectstatic : nécessaire pour servir proprement les fichiers statiques
# (aucun fichier statique custom pour l'instant, Chart.js est chargé
# depuis un CDN, mais on le garde pour une évolution future du dashboard).
RUN python manage.py collectstatic --noinput

EXPOSE 8000

# En développement : runserver suffit. En production, remplacer par :
#   gunicorn dashboard.wsgi:application --bind 0.0.0.0:8000 --workers 3
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
