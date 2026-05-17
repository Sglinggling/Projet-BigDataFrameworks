# Documentation API — Food Nutrition

API REST sécurisée par JWT, construite avec FastAPI.  
URL de base : `http://localhost:8000`  
Documentation interactive : `http://localhost:8000/docs`

---

## Authentification JWT

### Workflow complet

```
1. POST /auth/register   → créer un compte (JSON)
2. POST /auth/token      → obtenir un JWT Bearer (form-data)
3. Inclure le token dans chaque requête : Authorization: Bearer <token>
```

Le token JWT expire après 30 minutes. Répéter l'étape 2 pour en obtenir un nouveau.

### Étape 1 — Créer un compte

```bash
curl -X POST http://localhost:8000/auth/register \
     -H "Content-Type: application/json" \
     -d '{"username": "monuser", "password": "monmotdepasse"}'
```

Réponse :
```json
{
  "message": "Utilisateur 'monuser' créé avec succès"
}
```

### Étape 2 — Obtenir un token

```bash
curl -X POST http://localhost:8000/auth/token \
     -d "username=monuser&password=monmotdepasse"
```

Réponse :
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

Conserver le `access_token` — il sera passé comme `Bearer` dans toutes les requêtes suivantes.

---

## Endpoints

### Santé et informations

#### `GET /`

Message de bienvenue avec liens utiles.

```bash
curl http://localhost:8000/
```

Réponse :
```json
{
  "message": "Food Nutrition API v1.0.0",
  "docs": "http://localhost:8000/docs",
  "health": "http://localhost:8000/health"
}
```

---

#### `GET /health`

Vérifie la connexion à PostgreSQL. Utile pour les health checks Docker/CI.

```bash
curl http://localhost:8000/health
```

Réponse :
```json
{
  "status": "ok",
  "database": "connected"
}
```

---

### Datamart Sugar — `dm_sugar_by_category`

Top 10 produits les plus sucrés par catégorie alimentaire.

#### `GET /datamart/sugar`

Liste paginée des produits avec rang de teneur en sucre.

**Paramètres de requête :**

| Paramètre | Type | Défaut | Description |
|---|---|---|---|
| `page` | int | 1 | Numéro de page (commence à 1) |
| `limit` | int | 50 | Résultats par page (max 500) |
| `category` | string | — | Filtrer par catégorie (exact match) |
| `min_sugars` | float | — | Teneur en sucre minimale (g/100g) |
| `nutriscore_grade` | string | — | Grade Nutri-Score : a / b / c / d / e |

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/sugar?page=1&limit=10&nutriscore_grade=e"
```

Réponse :
```json
{
  "page": 1,
  "limit": 10,
  "total": 847,
  "items": [
    {
      "category": "Biscuits and cakes",
      "product_name": "Chocolate Cookies",
      "sugars_100g": 52.3,
      "nutrition_grade_fr": "e",
      "sugar_rank_in_category": 1
    }
  ]
}
```

---

#### `GET /datamart/sugar/categories`

Liste des catégories distinctes disponibles dans le datamart.

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/sugar/categories"
```

Réponse :
```json
{
  "categories": ["Biscuits and cakes", "Beverages", "Cereals", "..."],
  "count": 312
}
```

---

### Datamart Nutriscore — `dm_nutriscore_by_country`

Répartition du Nutri-Score par pays et région géographique.

#### `GET /datamart/nutriscore`

Liste paginée Nutri-Score par pays.

**Paramètres de requête :**

| Paramètre | Type | Défaut | Description |
|---|---|---|---|
| `page` | int | 1 | Numéro de page |
| `limit` | int | 50 | Résultats par page (max 500) |
| `country` | string | — | Filtrer par pays (insensible à la casse) |
| `region` | string | — | Filtrer par région géographique |
| `nutriscore_grade` | string | — | Grade Nutri-Score : a / b / c / d / e |

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/nutriscore?country=France&page=1&limit=5"
```

Réponse :
```json
{
  "page": 1,
  "limit": 5,
  "total": 5,
  "items": [
    {
      "country": "France",
      "region": "WESTERN EUROPE",
      "nutrition_grade_fr": "a",
      "nb_products": 1243,
      "avg_nutriscore_score": -3.2
    }
  ]
}
```

---

#### `GET /datamart/nutriscore/summary`

Agrégats globaux par grade Nutri-Score (toutes régions confondues).

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/nutriscore/summary"
```

Réponse :
```json
{
  "summary": [
    {"grade": "a", "count": 4521, "global_avg_score": -4.1},
    {"grade": "b", "count": 3102, "global_avg_score": 1.8},
    {"grade": "c", "count": 2876, "global_avg_score": 7.4},
    {"grade": "d", "count": 1984, "global_avg_score": 14.2},
    {"grade": "e", "count": 1103, "global_avg_score": 23.7}
  ]
}
```

---

### Datamart Additives — `dm_additives_analysis`

Additifs alimentaires les plus fréquents (explode de la colonne `additives_tags`).

#### `GET /datamart/additives`

Liste paginée des additifs triés par fréquence décroissante.

**Paramètres de requête :**

| Paramètre | Type | Défaut | Description |
|---|---|---|---|
| `page` | int | 1 | Numéro de page |
| `limit` | int | 50 | Résultats par page (max 500) |

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/additives?page=1&limit=5"
```

Réponse :
```json
{
  "page": 1,
  "limit": 5,
  "total": 487,
  "items": [
    {
      "additive_tag": "en:e330",
      "total_occurrences": 48392,
      "nb_distinct_categories": 87,
      "rank_overall": 1,
      "pct_of_total": 18.4200
    }
  ]
}
```

---

#### `GET /datamart/additives/top/{n}`

Retourne les N additifs de rang 1 à N directement (sans pagination).

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/additives/top/20"
```

Réponse : liste de 20 objets `AdditiveItem` (même structure que ci-dessus).

---

### Datamart ML — `dm_ml_nutriscore_prediction`

Dataset ML-ready : une ligne par produit unique avec features numériques et grade cible.

#### `GET /datamart/ml`

Dataset ML paginé, trié par code produit.

**Paramètres de requête :**

| Paramètre | Type | Défaut | Description |
|---|---|---|---|
| `page` | int | 1 | Numéro de page |
| `limit` | int | 100 | Résultats par page (max 1000) |

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/ml?page=1&limit=3"
```

Réponse :
```json
{
  "page": 1,
  "limit": 3,
  "total": 187432,
  "items": [
    {
      "code": "0000000000017",
      "sugars_100g": 4.2,
      "fat_100g": 3.5,
      "saturated_fat_100g": 1.1,
      "salt_100g": 0.8,
      "energy_100g": 245.0,
      "proteins_100g": 8.3,
      "fiber_100g": 1.2,
      "additives_n": 2,
      "main_category": "Dairy products",
      "country_normalized": "france",
      "population": 60876136,
      "area_sq_mi": 547030,
      "nutrition_grade_fr": "b"
    }
  ]
}
```

---

#### `GET /datamart/ml/stats`

Statistiques descriptives par grade Nutri-Score (moyennes de toutes les features).

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/ml/stats"
```

Réponse :
```json
[
  {
    "grade": "a",
    "count": 43210,
    "avg_energy": 142.3,
    "avg_fat": 2.1,
    "avg_sugars": 3.4,
    "avg_proteins": 9.2,
    "avg_fiber": 3.1,
    "avg_salt": 0.3,
    "avg_additives": 0.8
  }
]
```

---

#### `GET /datamart/ml/sample`

Retourne N lignes aléatoires du dataset ML (non reproductible).

**Paramètres de requête :**

| Paramètre | Type | Défaut | Description |
|---|---|---|---|
| `n` | int | 10 | Taille de l'échantillon (max 100) |

```bash
curl -H "Authorization: Bearer <TOKEN>" \
     "http://localhost:8000/datamart/ml/sample?n=5"
```

---

## Pagination — format commun

Tous les endpoints paginés retournent un objet `Page` :

```json
{
  "page":  1,
  "limit": 50,
  "total": 1234,
  "items": [ ... ]
}
```

- `total` : nombre total de résultats correspondant aux filtres actifs
- `page` : numéro de la page courante (commence à 1)
- `limit` : taille de la page demandée
- Nombre de pages : `ceil(total / limit)`

Exemple de navigation : `?page=2&limit=100` pour obtenir les lignes 101 à 200.

---

## Codes d'erreur

| Code HTTP | Cause |
|---|---|
| 401 | Token absent, invalide ou expiré |
| 409 | Nom d'utilisateur déjà pris (register) |
| 422 | Paramètre de requête invalide (type, plage) |
| 500 | Erreur PostgreSQL (base indisponible) |
