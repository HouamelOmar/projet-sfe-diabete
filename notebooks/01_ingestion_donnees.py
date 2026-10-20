# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingestion des données sources (couche Bronze - fichiers bruts)
# MAGIC
# MAGIC **Projet : Analyse concurrentielle du marché du diabète en France (2021-2025)**
# MAGIC
# MAGIC Ce notebook télécharge automatiquement les données publiques officielles
# MAGIC et les dépose, sans modification, dans un *Volume* Databricks.
# MAGIC
# MAGIC ⚠️ **Databricks Free Edition bloque l'accès à Internet** (test de la section 1 en échec).
# MAGIC Sur la version gratuite, l'ingestion est donc faite par **GitHub Actions**
# MAGIC (`.github/workflows/ingestion.yml` + `scripts/preparer_donnees.py`) : les données sont
# MAGIC téléchargées dans le cloud GitHub puis déposées dans le Volume via l'API Databricks.
# MAGIC Ce notebook reste utilisable tel quel sur un espace Databricks payant (entreprise).
# MAGIC
# MAGIC | Source | Producteur | Rôle dans l'étude (équivalent privé) |
# MAGIC |---|---|---|
# MAGIC | Medic'AM (mensuel, 2021-2025) | Assurance Maladie | Ventes des médicaments (≈ GERS) |
# MAGIC | Base publique des médicaments | Ministère de la Santé / ANSM | Lien médicament → laboratoire |
# MAGIC | Transparence Santé | Ministère de la Santé | Actions des labos auprès des médecins |
# MAGIC | Annuaire Santé RPPS | Agence du Numérique en Santé | Fichier des médecins (≈ OneKey) |
# MAGIC
# MAGIC **Protection des données (RGPD)** : on ne télécharge ni les noms ni les adresses des médecins
# MAGIC depuis Transparence Santé, seulement leur numéro RPPS et leur code postal.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Paramètres

# COMMAND ----------

CATALOGUE = "workspace"          # catalogue par défaut de Databricks Free Edition
SCHEMA = "sfe_diabete"
VOLUME = "brut"

ANNEES = [2021, 2022, 2023, 2024, 2025]

# Laboratoires du diabète (noms exacts des filiales françaises dans Transparence Santé)
LABOS_TRANSPARENCE = [
    "NOVO NORDISK",
    "SANOFI WINTHROP INDUSTRIE", "SANOFI AVENTIS FRANCE",
    "LILLY FRANCE", "LILLY FRANCE SAS",
    "ASTRAZENECA",
    "BOEHRINGER INGELHEIM FRANCE",
    "MSD FRANCE",
    "LES LABORATOIRES SERVIER",
]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGUE}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGUE}.{SCHEMA}.{VOLUME}")
CHEMIN = f"/Volumes/{CATALOGUE}/{SCHEMA}/{VOLUME}"
print("Dossier de destination :", CHEMIN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Test de connexion Internet
# MAGIC
# MAGIC La version gratuite de Databricks peut bloquer certains sites.
# MAGIC Si un site est ❌, il faudra importer ses fichiers à la main (voir README).

# COMMAND ----------

import requests

SITES = {
    "Assurance Maladie (Medic'AM)": "https://www.assurance-maladie.ameli.fr",
    "Base publique des médicaments": "https://base-donnees-publique.medicaments.gouv.fr",
    "Transparence Santé": "https://www.transparence.sante.gouv.fr/api/explore/v2.1/catalog/datasets",
    "data.gouv.fr (RPPS)": "https://www.data.gouv.fr/api/1/datasets/",
}

for nom, url in SITES.items():
    try:
        code = requests.get(url, timeout=20).status_code
        print(f"{'✅' if code < 400 else '❌'} {nom} (code {code})")
    except Exception as erreur:
        print(f"❌ {nom} : {type(erreur).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fonction de téléchargement

# COMMAND ----------

import os
import zipfile

def telecharger(url, destination, params=None):
    """Télécharge un fichier par morceaux (gère les gros fichiers) et renvoie sa taille en Mo."""
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    with requests.get(url, params=params, stream=True, timeout=600) as reponse:
        reponse.raise_for_status()
        with open(destination, "wb") as f:
            for morceau in reponse.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(morceau)
    taille = os.path.getsize(destination) / 1e6
    print(f"  {os.path.basename(destination)} : {taille:.1f} Mo")
    return taille

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Medic'AM : ventes mensuelles de médicaments (Assurance Maladie)
# MAGIC
# MAGIC Un fichier par semestre, contenant les remboursements **mois par mois** et par type de
# MAGIC prescripteur (tous / ville / hôpital).
# MAGIC
# MAGIC ⚠️ Sur data.gouv.fr, le lien « 2e semestre 2025 » pointe par erreur vers le 1er semestre :
# MAGIC on construit donc les adresses nous-mêmes à partir du format officiel.

# COMMAND ----------

def url_medicam(annee, periode, suffixe=""):
    return (f"https://www.assurance-maladie.ameli.fr/sites/default/files/"
            f"{annee}-{periode}_medic-am-par-type-de-prescripteur_serie-mensuelle{suffixe}.zip")

for annee in ANNEES:
    for periode in ["01-a-06", "07-a-12"]:
        fichier_zip = f"{CHEMIN}/medicam/{annee}-{periode}.zip"
        for suffixe in ["", "_0"]:          # certains fichiers ont un suffixe "_0" sur le site
            try:
                telecharger(url_medicam(annee, periode, suffixe), fichier_zip)
                with zipfile.ZipFile(fichier_zip) as z:
                    z.extractall(f"{CHEMIN}/medicam/")
                os.remove(fichier_zip)
                break
            except (requests.HTTPError, zipfile.BadZipFile):
                continue
        else:
            raise RuntimeError(f"Medic'AM {annee} {periode} introuvable")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Base publique des médicaments (lien médicament → laboratoire)

# COMMAND ----------

# CIS_bdpm : médicaments et laboratoire titulaire
# CIS_CIP_bdpm : présentations (boîtes) avec leur code CIP13, la clé commune avec Medic'AM
# CIS_GENER_bdpm : groupes génériques (permet de distinguer médicament de marque / générique)
for nom in ["CIS_bdpm.txt", "CIS_CIP_bdpm.txt", "CIS_GENER_bdpm.txt"]:
    telecharger(f"https://base-donnees-publique.medicaments.gouv.fr/download/file/{nom}",
                f"{CHEMIN}/bdpm/{nom}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transparence Santé : déclarations des laboratoires du diabète vers les médecins
# MAGIC
# MAGIC Filtres appliqués à la source :
# MAGIC - les 7 laboratoires du diabète (filiales françaises) ;
# MAGIC - bénéficiaires **médecins** uniquement (code profession 10) ;
# MAGIC - période **2021-2025**.

# COMMAND ----------

liste_labos = ", ".join(f'"{l}"' for l in LABOS_TRANSPARENCE)
filtre = (f"raison_sociale in ({liste_labos}) "
          f"and date >= date'{ANNEES[0]}-01-01' and date < date'{ANNEES[-1] + 1}-01-01' "
          f"and beneficiaire_profession_code = \"10\"")

colonnes = ("id,raison_sociale,numero_siren,lien_interet,motif_lien_interet,autre_motif,"
            "information_evenement,convention_liee,montant,date,date_debut,date_fin,"
            "beneficiaire_identifiant,beneficiaire_type_code,beneficiaire_profession_code,"
            "code_postal,pays_code,statut")   # pas de nom ni d'adresse (RGPD)

telecharger(
    "https://www.transparence.sante.gouv.fr/api/explore/v2.1/catalog/datasets/declarations/exports/csv",
    f"{CHEMIN}/transparence/declarations_labos_diabete.csv",
    params={"select": colonnes, "where": filtre, "delimiter": ";"},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Annuaire Santé RPPS : fichier des professionnels de santé
# MAGIC
# MAGIC Fichier national mis à jour chaque jour (≈ 830 Mo). On récupère l'adresse de la
# MAGIC dernière version via l'API de data.gouv.fr.

# COMMAND ----------

jeu_rpps = requests.get(
    "https://www.data.gouv.fr/api/1/datasets/annuaire-sante-extractions-des-donnees-en-libre-acces-des-professionnels-intervenant-dans-le-systeme-de-sante-rpps/",
    timeout=60,
).json()
url_rpps = next(r["url"] for r in jeu_rpps["resources"] if "personne-activite" in r["title"])
telecharger(url_rpps, f"{CHEMIN}/rpps/personne_activite.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Contrôle final

# COMMAND ----------

for dossier in ["medicam", "bdpm", "transparence", "rpps"]:
    fichiers = dbutils.fs.ls(f"{CHEMIN}/{dossier}")
    total = sum(f.size for f in fichiers) / 1e6
    print(f"{dossier:13s} {len(fichiers):3d} fichier(s)  {total:8.1f} Mo")
