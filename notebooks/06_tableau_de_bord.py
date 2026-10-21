# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Tableau de bord AI/BI
# MAGIC
# MAGIC Ce notebook crée (ou met à jour) le tableau de bord **« Marché du diabète - SFE »** dans Databricks AI/BI :
# MAGIC 1. il prépare des **vues** `dash_*` aux noms lisibles, calculées à partir des tables Gold ;
# MAGIC 2. il construit le tableau de bord (3 pages) et le **publie** via l'API Databricks.
# MAGIC
# MAGIC | Page | Contenu |
# MAGIC |---|---|
# MAGIC | Marché | chiffres clés, ventes par classe, parts de marché, gagnants et perdants |
# MAGIC | Investissements | montants par laboratoire, type d'action, thème, spécialité et région, avec filtres |
# MAGIC | Ciblage et positionnement | couverture des endocrinologues, intensité promotionnelle, investissement et ventes |
# MAGIC
# MAGIC Le tableau de bord est rangé dans le dossier personnel de l'utilisateur (Workspace > Users > …) et
# MAGIC s'actualise à chaque exécution du job (notebook 04 puis 06). Les couleurs des laboratoires sont les mêmes
# MAGIC que dans le notebook 05.

# COMMAND ----------

import json

CATALOGUE_SCHEMA = "workspace.sfe_diabete"
NOM_TABLEAU_DE_BORD = "Marché du diabète - SFE"
spark.sql(f"USE {CATALOGUE_SCHEMA}")

COULEURS = {
    "Novo Nordisk": "#2a78d6", "AstraZeneca": "#eb6834", "Lilly": "#1baf7a", "Boehringer Ingelheim": "#eda100",
    "Sanofi": "#e87ba4", "Servier": "#008300", "MSD": "#6250d6", "Autres laboratoires": "#a3a29c",
}
NOMS_SQL = """CASE groupe_laboratoire
    WHEN 'NOVO NORDISK' THEN 'Novo Nordisk' WHEN 'ASTRAZENECA' THEN 'AstraZeneca' WHEN 'LILLY' THEN 'Lilly'
    WHEN 'BOEHRINGER INGELHEIM' THEN 'Boehringer Ingelheim' WHEN 'SANOFI' THEN 'Sanofi'
    WHEN 'SERVIER' THEN 'Servier' WHEN 'MSD' THEN 'MSD' ELSE 'Autres laboratoires' END"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vues pour le tableau de bord

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW dash_ventes AS
SELECT date_mois, annee, semestre, classe_therapeutique, {NOMS_SQL} AS laboratoire, produit, type_produit,
       ventes_eur / 1e6 AS ventes_meur, nb_boites
FROM gold_ventes_mensuelles
""")

spark.sql(f"""
CREATE OR REPLACE VIEW dash_parts_de_marche AS
SELECT semestre, {NOMS_SQL} AS laboratoire,
       sum(ventes_eur) / 1e6 AS ventes_meur, 100 * sum(part_de_marche) AS part_de_marche_pct
FROM gold_parts_de_marche
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE VIEW dash_evolution_parts AS
WITH bornes AS (SELECT min(semestre) AS debut, max(semestre) AS fin FROM dash_parts_de_marche)
SELECT p.laboratoire,
       sum(CASE WHEN p.semestre = b.fin THEN p.part_de_marche_pct END)
       - sum(CASE WHEN p.semestre = b.debut THEN p.part_de_marche_pct END) AS evolution_part_points
FROM dash_parts_de_marche p CROSS JOIN bornes b
WHERE p.laboratoire <> 'Autres laboratoires'
GROUP BY p.laboratoire
""")

spark.sql(f"""
CREATE OR REPLACE VIEW dash_investissements AS
SELECT annee, semestre, {NOMS_SQL} AS laboratoire, categorie_action, theme_evenement, specialite, region,
       CASE WHEN cible_diabete THEN 'Diabète' ELSE 'Hors diabète' END AS ciblage,
       montant_verse_eur / 1e3 AS montant_verse_keur, nb_declarations
FROM gold_investissements_detail
""")

spark.sql(f"""
CREATE OR REPLACE VIEW dash_impact AS
SELECT semestre, {NOMS_SQL} AS laboratoire,
       ventes_eur / 1e6 AS ventes_meur, 100 * part_de_marche AS part_de_marche_pct,
       invest_diabete_eur / 1e3 AS invest_diabete_keur, intensite_promo_pour_1000_eur,
       100 * couverture_endocrinologues AS couverture_endocrinologues_pct,
       100 * evolution_invest_diabete_annuelle AS evolution_invest_diabete_pct,
       100 * evolution_ventes_semestre_suivant AS evolution_ventes_semestre_suivant_pct
FROM gold_impact
""")

for vue in ["dash_ventes", "dash_parts_de_marche", "dash_evolution_parts", "dash_investissements", "dash_impact"]:
    print(f"{vue} : {spark.table(vue).count():,} lignes".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Définition du tableau de bord
# MAGIC
# MAGIC Format « Lakeview » de Databricks AI/BI : des **jeux de données** (requêtes SQL sur les vues) et des
# MAGIC **pages** composées de widgets placés sur une grille de 6 colonnes.

# COMMAND ----------

def jeu_de_donnees(nom, titre, requete):
    return {"name": nom, "displayName": titre, "queryLines": [requete]}

def champ(nom, expression=None):
    return {"name": nom, "expression": expression or f"`{nom}`"}

def place(widget, x, y, largeur, hauteur):
    return {"widget": widget, "position": {"x": x, "y": y, "width": largeur, "height": hauteur}}

def texte(nom, markdown):
    return {"name": nom, "multilineTextboxSpec": {"lines": [markdown]}}

def couleurs_laboratoires(champ_couleur):
    return {"fieldName": champ_couleur, "displayName": "Laboratoire",
            "scale": {"type": "categorical",
                      "mappings": [{"value": nom, "color": couleur} for nom, couleur in COULEURS.items()]}}

def graphique(nom, type_graphique, jeu, x, y, titre, couleur=None, x_titre=None, y_titre=None):
    """Courbe ou barres : x en catégories, somme de y, couleur facultative."""
    y_champ = f"sum({y})"
    champs = [champ(x), champ(y_champ, f"SUM(`{y}`)")] + ([champ(couleur)] if couleur else [])
    encodages = {
        "x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x_titre or x},
        "y": {"fieldName": y_champ, "scale": {"type": "quantitative"}, "displayName": y_titre or y},
    }
    if couleur:
        encodages["color"] = (couleurs_laboratoires(couleur) if couleur == "laboratoire"
                              else {"fieldName": couleur, "scale": {"type": "categorical"}, "displayName": couleur})
    return {
        "name": nom,
        "queries": [{"name": "main_query", "query": {"datasetName": jeu, "fields": champs, "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": type_graphique, "encodings": encodages,
                 "frame": {"showTitle": True, "title": titre}},
    }

def compteur(nom, jeu, valeur, titre, distinct=False):
    valeur_champ = f"countdistinct({valeur})" if distinct else f"sum({valeur})"
    expression = f"COUNT(DISTINCT `{valeur}`)" if distinct else f"SUM(`{valeur}`)"
    return {
        "name": nom,
        "queries": [{"name": "main_query", "query": {
            "datasetName": jeu, "fields": [champ(valeur_champ, expression)], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": valeur_champ, "displayName": titre}},
                 "frame": {"showTitle": True, "title": titre}},
    }

def filtre(nom, jeu, colonne, titre):
    requete = f"filtre_{jeu}_{colonne}"
    return {
        "name": nom,
        "queries": [{"name": requete, "query": {"datasetName": jeu, "fields": [champ(colonne)], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "filter-multi-select",
                 "encodings": {"fields": [{"fieldName": colonne, "displayName": titre, "queryName": requete}]},
                 "frame": {"showTitle": True, "title": titre}},
    }

# COMMAND ----------

V = CATALOGUE_SCHEMA
jeux = [
    jeu_de_donnees("ventes", "Ventes", f"SELECT * FROM {V}.dash_ventes"),
    jeu_de_donnees("ventes_derniere_annee", "Ventes de la dernière année",
                   f"SELECT * FROM {V}.dash_ventes WHERE annee = (SELECT max(annee) FROM {V}.dash_ventes)"),
    jeu_de_donnees("parts", "Parts de marché", f"SELECT * FROM {V}.dash_parts_de_marche"),
    jeu_de_donnees("evolution_parts", "Évolution des parts de marché", f"SELECT * FROM {V}.dash_evolution_parts"),
    jeu_de_donnees("investissements", "Investissements", f"SELECT * FROM {V}.dash_investissements"),
    jeu_de_donnees("impact", "Investissement et ventes", f"SELECT * FROM {V}.dash_impact"),
    jeu_de_donnees("impact_dernier_semestre", "Dernier semestre",
                   f"SELECT * FROM {V}.dash_impact WHERE semestre = (SELECT max(semestre) FROM {V}.dash_impact)"),
]

page_marche = [
    place(texte("titre_marche", "# Marché des antidiabétiques en France, 2021-2025\n"
                "Ventes = base de remboursement Medic'AM (pharmacies de ville). Source : Assurance Maladie."), 0, 0, 6, 2),
    place(compteur("ventes_annee", "ventes_derniere_annee", "ventes_meur", "Ventes de la dernière année (M€)"), 0, 2, 2, 3),
    place(compteur("boites_annee", "ventes_derniere_annee", "nb_boites", "Boîtes remboursées, dernière année"), 2, 2, 2, 3),
    place(compteur("produits_annee", "ventes_derniere_annee", "produit", "Produits vendus, dernière année",
                   distinct=True), 4, 2, 2, 3),
    place(graphique("ventes_classes", "line", "ventes", "annee", "ventes_meur", "Ventes par classe thérapeutique (M€)",
                    couleur="classe_therapeutique", x_titre="Année", y_titre="Ventes (M€)"), 0, 5, 3, 7),
    place(graphique("ventes_labos", "bar", "ventes", "annee", "ventes_meur", "Ventes par laboratoire (M€)",
                    couleur="laboratoire", x_titre="Année", y_titre="Ventes (M€)"), 3, 5, 3, 7),
    place(graphique("parts_semestre", "line", "parts", "semestre", "part_de_marche_pct",
                    "Part de marché par semestre (%)", couleur="laboratoire", x_titre="Semestre",
                    y_titre="Part de marché (%)"), 0, 12, 4, 8),
    place(graphique("gagnants_perdants", "bar", "evolution_parts", "laboratoire", "evolution_part_points",
                    "Évolution de la part de marché sur la période (points)", couleur="laboratoire",
                    x_titre="Laboratoire", y_titre="Points de part de marché"), 4, 12, 2, 8),
]

page_investissements = [
    place(texte("titre_invest", "# Investissements des laboratoires auprès des médecins\n"
                "Avantages et rémunérations versés (Transparence Santé). « Diabète » : événement sur le diabète, "
                "ou endocrinologue si l'événement n'est pas précisé."), 0, 0, 6, 2),
    place(filtre("f_labo", "investissements", "laboratoire", "Laboratoire"), 0, 2, 1, 2),
    place(filtre("f_annee", "investissements", "annee", "Année"), 1, 2, 1, 2),
    place(filtre("f_ciblage", "investissements", "ciblage", "Ciblage"), 2, 2, 1, 2),
    place(filtre("f_theme", "investissements", "theme_evenement", "Thème de l'événement"), 3, 2, 1, 2),
    place(filtre("f_specialite", "investissements", "specialite", "Spécialité du médecin"), 4, 2, 1, 2),
    place(filtre("f_region", "investissements", "region", "Région"), 5, 2, 1, 2),
    place(graphique("invest_annee", "bar", "investissements", "annee", "montant_verse_keur",
                    "Montants versés par laboratoire (k€)", couleur="laboratoire", x_titre="Année",
                    y_titre="Montant versé (k€)"), 0, 4, 3, 7),
    place(graphique("invest_categorie", "bar", "investissements", "categorie_action", "montant_verse_keur",
                    "Par type d'action (k€)", couleur="laboratoire", x_titre="Type d'action",
                    y_titre="Montant versé (k€)"), 3, 4, 3, 7),
    place(graphique("invest_specialite", "bar", "investissements", "specialite", "montant_verse_keur",
                    "Par spécialité du médecin (k€)", couleur="laboratoire", x_titre="Spécialité",
                    y_titre="Montant versé (k€)"), 0, 11, 3, 7),
    place(graphique("invest_region", "bar", "investissements", "region", "montant_verse_keur",
                    "Par région (k€)", couleur="laboratoire", x_titre="Région",
                    y_titre="Montant versé (k€)"), 3, 11, 3, 7),
    place(graphique("invest_theme", "bar", "investissements", "theme_evenement", "montant_verse_keur",
                    "Par thème de l'événement (k€)", couleur="laboratoire", x_titre="Thème",
                    y_titre="Montant versé (k€)"), 0, 18, 6, 6),
]

page_ciblage = [
    place(texte("titre_ciblage", "# Ciblage et positionnement\n"
                "Couverture : part des endocrinologues en activité touchés dans le semestre. Intensité : € investis "
                "(diabète) pour 1 000 € de ventes. Les rapprochements investissement / ventes sont descriptifs."), 0, 0, 6, 2),
    place(filtre("f_labo_impact", "impact", "laboratoire", "Laboratoire"), 0, 2, 2, 2),
    place(graphique("couverture", "line", "impact", "semestre", "couverture_endocrinologues_pct",
                    "Couverture des endocrinologues (%)", couleur="laboratoire", x_titre="Semestre",
                    y_titre="Couverture (%)"), 0, 4, 3, 7),
    place(graphique("invest_diabete", "line", "impact", "semestre", "invest_diabete_keur",
                    "Investissement diabète par semestre (k€)", couleur="laboratoire", x_titre="Semestre",
                    y_titre="Investissement diabète (k€)"), 3, 4, 3, 7),
    place(graphique("intensite", "bar", "impact_dernier_semestre", "laboratoire", "intensite_promo_pour_1000_eur",
                    "Intensité promotionnelle, dernier semestre (€ pour 1 000 € de ventes)", couleur="laboratoire",
                    x_titre="Laboratoire", y_titre="€ pour 1 000 € de ventes"), 0, 11, 3, 7),
    place(graphique("part_dernier", "bar", "impact_dernier_semestre", "laboratoire", "part_de_marche_pct",
                    "Part de marché, dernier semestre (%)", couleur="laboratoire", x_titre="Laboratoire",
                    y_titre="Part de marché (%)"), 3, 11, 3, 7),
]

tableau_de_bord = {
    "datasets": jeux,
    "pages": [
        {"name": "marche", "displayName": "Marché", "pageType": "PAGE_TYPE_CANVAS", "layout": page_marche},
        {"name": "investissements", "displayName": "Investissements", "pageType": "PAGE_TYPE_CANVAS",
         "layout": page_investissements},
        {"name": "ciblage", "displayName": "Ciblage et positionnement", "pageType": "PAGE_TYPE_CANVAS",
         "layout": page_ciblage},
    ],
}

# Contrôle : chaque widget interroge un jeu de données existant
noms_jeux = {j["name"] for j in jeux}
for page in tableau_de_bord["pages"]:
    for element in page["layout"]:
        for requete in element["widget"].get("queries", []):
            assert requete["query"]["datasetName"] in noms_jeux, requete["query"]["datasetName"]
print(f"{sum(len(p['layout']) for p in tableau_de_bord['pages'])} widgets sur {len(tableau_de_bord['pages'])} pages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Création ou mise à jour, puis publication
# MAGIC
# MAGIC Le tableau de bord utilise le premier **SQL warehouse** de l'espace (sur Databricks Free Edition :
# MAGIC « Serverless Starter Warehouse »). La publication avec `embed_credentials` permet de le consulter sans
# MAGIC relancer les requêtes à la main.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
api = w.api_client

entrepots = api.do("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
if not entrepots:
    raise RuntimeError("Aucun SQL warehouse : en créer un (menu SQL Warehouses), puis relancer.")
entrepot_id = entrepots[0]["id"]
print("SQL warehouse :", entrepots[0]["name"])

def tableaux_de_bord_existants():
    page, jeton = [], None
    while True:
        reponse = api.do("GET", "/api/2.0/lakeview/dashboards",
                         query={"page_size": 100, **({"page_token": jeton} if jeton else {})})
        page += reponse.get("dashboards", [])
        jeton = reponse.get("next_page_token")
        if not jeton:
            return page

corps = {"display_name": NOM_TABLEAU_DE_BORD, "warehouse_id": entrepot_id,
         "serialized_dashboard": json.dumps(tableau_de_bord, ensure_ascii=False)}
existant = next((d for d in tableaux_de_bord_existants() if d.get("display_name") == NOM_TABLEAU_DE_BORD), None)
if existant:
    identifiant = existant["dashboard_id"]
    api.do("PATCH", f"/api/2.0/lakeview/dashboards/{identifiant}", body=corps)
    print("Tableau de bord mis à jour")
else:
    corps["parent_path"] = f"/Workspace/Users/{w.current_user.me().user_name}"
    identifiant = api.do("POST", "/api/2.0/lakeview/dashboards", body=corps)["dashboard_id"]
    print("Tableau de bord créé")

api.do("POST", f"/api/2.0/lakeview/dashboards/{identifiant}/published",
       body={"embed_credentials": True, "warehouse_id": entrepot_id})
lien = f"{w.config.host.rstrip('/')}/dashboardsv3/{identifiant}/published"
displayHTML(f'<p>Tableau de bord publié : <a href="{lien}" target="_blank">{NOM_TABLEAU_DE_BORD}</a></p>')
