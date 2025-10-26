# SNCB Slac

Collecte et affichage des trajets Tournai ↔︎ Bruxelles-Central via l'API iRail. 
- **Persistance** uniquement des trajets **arrivés** au terminus.
- **Front** affiche les trains **en cours** ou **déjà stockés**.
- **Chaque train** affiche **tous les arrêts**.

## Démarrage rapide

```bash
cp .env.example .env
# Éventuellement éditer les mots de passe et NEXT_PUBLIC_API_BASE_URL
docker compose build
docker compose up -d
```

Backend: http://localhost:8000/docs  
Frontend: http://localhost:3000

### Données d'exemple
Pour insérer un jeu d’essai :
```bash
docker compose exec backend python /app/example_data.py
```
Pour **supprimer** ces données d’exemple :
```bash
docker compose exec backend python -c "from example_data import wipe_example; wipe_example()"
```

## Déploiement Dokploy (sncb.terminalcommun.be)
- Ajoutez ce dépôt Git dans Dokploy (App Compose).
- Configurez les variables `.env` dans l’interface.
- Mappez un domaine vers le **frontend** (3000) et un vers le **backend** (8000) si nécessaire.
- Activez TLS.

## Architecture
- **backend/** FastAPI + SQLAlchemy (MySQL/MariaDB).
- **worker/** collecte iRail toutes les 2 min (env `POLL_SECONDS`).
- **frontend/** Next.js + Tailwind.

## Sécurité & bonnes pratiques
- API publique **lecture seule** (aucune écriture exposée).
- Limites iRail respectées (cadence, User-Agent, timeouts).
- Identifiants `vehicle_uri` + `service_date` pour l'idempotence.

## Licence
MIT
