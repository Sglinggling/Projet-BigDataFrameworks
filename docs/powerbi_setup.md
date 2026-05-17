# Guide Power BI Service — Import CSV et création des dashboards

Ce guide est destiné aux utilisateurs sur macOS. Power BI Desktop n'étant pas disponible
sur macOS, on utilise Power BI Service (version web) avec import de fichiers CSV.

URL Power BI Service : https://app.powerbi.com

---

## Prérequis

- Un compte Microsoft ou compte professionnel/scolaire avec accès Power BI Service
- Les 4 fichiers CSV générés par `export_for_powerbi.py` dans `data/output/powerbi/` :
  - `dm_sugar_by_category.csv`
  - `dm_nutriscore_by_country.csv`
  - `dm_additives_analysis.csv`
  - `dm_ml_nutriscore_prediction.csv`

---

## Étape 1 — Générer les fichiers CSV

Depuis la racine du projet, les containers Docker étant démarrés :

```bash
python3 scripts/export_for_powerbi.py --config config --output data/output/powerbi
```

Vérifier la présence des 4 fichiers :

```bash
ls -lh data/output/powerbi/
```

---

## Étape 2 — Se connecter à Power BI Service

1. Ouvrir un navigateur et aller sur https://app.powerbi.com
2. Se connecter avec un compte Microsoft

[SCREENSHOT_1 : Page de connexion Power BI Service]

---

## Étape 3 — Créer un espace de travail (optionnel)

Si vous souhaitez organiser les rapports dans un espace dédié :

1. Dans le panneau gauche, cliquer sur **Espaces de travail**
2. Cliquer sur **+ Nouvel espace de travail**
3. Nommer l'espace : `Food Nutrition EFREI`
4. Cliquer sur **Enregistrer**

---

## Étape 4 — Importer les fichiers CSV

Pour chacun des 4 fichiers, répéter la procédure suivante :

1. Dans le panneau gauche, cliquer sur **Mon espace de travail** (ou l'espace créé)
2. Cliquer sur **+ Nouveau** → **Télécharger un fichier**
3. Sélectionner **Fichier local**
4. Choisir le fichier CSV correspondant dans `data/output/powerbi/`
5. Power BI détecte automatiquement l'encodage UTF-8 BOM et le séparateur virgule
6. Cliquer sur **Ouvrir** puis **Se connecter**

[SCREENSHOT_2 : Dialog d'import CSV — sélection du fichier]

[SCREENSHOT_3 : Aperçu des données CSV après détection automatique]

Répéter pour les 4 fichiers. Chaque fichier crée un **Dataset** dans Power BI.

---

## Étape 5 — Vérifier les types de colonnes

Après import, vérifier que Power BI a correctement inféré les types :

1. Dans **Mon espace de travail**, cliquer sur un dataset importé
2. Aller dans **Transformer les données** (bouton en haut)
3. Vérifier que les colonnes numériques sont bien en type **Nombre décimal** ou **Nombre entier**
4. Les colonnes `nutrition_grade_fr`, `country`, `category` doivent être en type **Texte**
5. Cliquer sur **Fermer et appliquer** si des corrections ont été effectuées

---

## Étape 6 — Créer les rapports

### Dashboard 1 — Carte choroplèthe Nutri-Score par pays

**Source :** `dm_nutriscore_by_country.csv`

**Objectif :** Visualiser le Nutri-Score moyen par pays sur une carte mondiale.

Construction :
1. Dans le dataset `dm_nutriscore_by_country`, cliquer sur **Créer un rapport**
2. Dans le volet **Visualisations**, sélectionner **Carte choroplèthe** (Filled Map)
3. Glisser-déposer :
   - `country` → champ **Emplacement**
   - `avg_nutriscore_score` → champ **Saturation des couleurs** (agrégation : Moyenne)
4. Dans le volet **Format** :
   - Thème de couleur : gradient vert (scores bas = grade A) → rouge (scores hauts = grade E)
   - Activer les **Étiquettes de données**
5. Ajouter un **Segment** sur `nutrition_grade_fr` pour filtrer par grade
6. Ajouter un **Segment** sur `region` pour filtrer par région géographique

[SCREENSHOT_4 : Carte choroplèthe Nutri-Score par pays]

---

### Dashboard 2 — Top produits sucrés par catégorie

**Source :** `dm_sugar_by_category.csv`

**Objectif :** Identifier les produits les plus sucrés dans chaque catégorie alimentaire.

Construction :
1. Dans le dataset `dm_sugar_by_category`, cliquer sur **Créer un rapport**
2. Ajouter un **Graphique à barres groupées** :
   - Axe X : `product_name`
   - Axe Y : `sugars_100g`
   - Légende : `nutrition_grade_fr`
3. Ajouter un **Segment** sur `category` (liste déroulante ou sélecteur unique)
4. Ajouter un **Segment** sur `sugar_rank_in_category` (filtre ≤ 10 pour garder le top 10)
5. Ajouter un **Tableau** avec les colonnes : `category`, `product_name`, `sugars_100g`, `nutrition_grade_fr`, `sugar_rank_in_category`
6. Activer la mise en surbrillance croisée entre le graphique et le tableau

[SCREENSHOT_5 : Top 10 produits sucrés avec filtre catégorie]

---

### Dashboard 3 — Analyse des additifs alimentaires

**Source :** `dm_additives_analysis.csv`

**Objectif :** Visualiser les additifs les plus fréquents et leur part dans le dataset.

Construction :
1. Dans le dataset `dm_additives_analysis`, cliquer sur **Créer un rapport**
2. Ajouter un **Graphique à barres horizontales** (Top 20) :
   - Axe Y : `additive_tag`
   - Axe X : `total_occurrences`
   - Filtre visuel : `rank_overall` ≤ 20
3. Ajouter un **Graphique en anneau** :
   - Légende : `additive_tag` (top 10)
   - Valeurs : `pct_of_total`
4. Ajouter des **Cartes KPI** :
   - Nombre total d'additifs distincts (`COUNT` de `additive_tag`)
   - Additif le plus fréquent (`rank_overall` = 1, `additive_tag`)
5. Ajouter un **Tableau** avec toutes les colonnes pour le détail

[SCREENSHOT_6 : Bar chart top 20 additifs avec graphique en anneau]

---

### Dashboard 4 — Distribution des grades Nutri-Score (ML)

**Source :** `dm_ml_nutriscore_prediction.csv`

**Objectif :** Explorer la distribution des grades et les corrélations entre features.

Construction :
1. Dans le dataset `dm_ml_nutriscore_prediction`, cliquer sur **Créer un rapport**
2. Ajouter un **Graphique à barres groupées** pour la distribution des grades :
   - Axe X : `nutrition_grade_fr`
   - Axe Y : `COUNT(*)`
   - Couleurs : vert (A) → rouge (E) (palette personnalisée)
3. Ajouter un **Nuage de points** (scatter) :
   - Axe X : `sugars_100g`
   - Axe Y : `energy_100g`
   - Légende/Couleur : `nutrition_grade_fr`
   - Taille : `fat_100g`
4. Ajouter des **Cartes KPI** pour les moyennes par feature :
   - Moyenne `sugars_100g`, `energy_100g`, `proteins_100g` (segmentées par grade via slicer)
5. Ajouter un **Segment** sur `nutrition_grade_fr` et `main_category`

[SCREENSHOT_7 : Distribution grades A-E + scatter sucres vs énergie]

---

## Étape 7 — Publier et partager

1. En haut de la page du rapport, cliquer sur **Fichier** → **Enregistrer**
2. Nommer le rapport : `Food Nutrition — Analyse alimentaire mondiale`
3. Pour partager : **Fichier** → **Incorporer le rapport** → copier le lien

Pour le rendu du projet, exporter chaque page en PDF :
**Fichier** → **Exporter** → **PDF**

---

## Rafraichissement des données

Les datasets CSV sont statiques (snapshot). Pour mettre à jour les données :

1. Relancer le pipeline complet (feeder → processor → datamart)
2. Relancer `export_for_powerbi.py`
3. Dans Power BI Service : **Mon espace de travail** → dataset → **...** → **Supprimer**
4. Réimporter le nouveau CSV (étape 4)

Power BI Service (version gratuite) ne supporte pas la programmation automatique de
rafraichissement pour des sources de fichiers locaux. La mise à jour est manuelle.
