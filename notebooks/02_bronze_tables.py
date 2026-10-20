# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Couche Bronze : chargement des fichiers bruts en tables Delta
# MAGIC
# MAGIC Les fichiers téléchargés par le notebook 01 sont chargés **tels quels** dans des tables Delta.
# MAGIC Seules transformations : noms de colonnes normalisés (sans accents ni espaces) et
# MAGIC mise en format « long » de Medic'AM (une ligne par médicament × mois × type de prescripteur).
# MAGIC
# MAGIC | Table créée | Source | Grain |
# MAGIC |---|---|---|
# MAGIC | `bronze_medicam` | Medic'AM | médicament (CIP13) × mois × type de prescripteur |
# MAGIC | `bronze_bdpm_specialites` | BDPM | médicament (CIS) |
# MAGIC | `bronze_bdpm_presentations` | BDPM | présentation / boîte (CIP13) |
# MAGIC | `bronze_bdpm_generiques` | BDPM | médicament (CIS) dans un groupe générique |
# MAGIC | `bronze_transparence` | Transparence Santé | déclaration |
# MAGIC | `bronze_rpps` | Annuaire Santé | professionnel × activité |

# COMMAND ----------

# MAGIC %pip install xlrd==2.0.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

CATALOGUE, SCHEMA = "workspace", "sfe_diabete"
CHEMIN = f"/Volumes/{CATALOGUE}/{SCHEMA}/brut"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGUE}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGUE}.{SCHEMA}.brut")
spark.sql(f"USE {CATALOGUE}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Vérification des fichiers importés dans le Volume

# COMMAND ----------

import os
import zipfile

# Fichier unique importé à la racine du Volume : on le décompresse dans les sous-dossiers
ARCHIVE = f"{CHEMIN}/donnees_sfe_diabete.zip"
if os.path.exists(ARCHIVE):
    with zipfile.ZipFile(ARCHIVE) as z:
        z.extractall(CHEMIN)
    print(f"{ARCHIVE} décompressé\n")

print(f"Contenu de {CHEMIN} :")
for racine, dossiers, fichiers in os.walk(CHEMIN):
    for f in sorted(fichiers):
        chemin_fichier = os.path.join(racine, f)
        print(f"  {chemin_fichier.replace(CHEMIN + '/', ''):60s} {os.path.getsize(chemin_fichier) / 1e6:7.1f} Mo")

ATTENDUS = {"medicam": (".zip", ".xls"), "bdpm": (".txt",), "transparence": (".csv.gz",), "rpps": (".txt.gz",)}
manquants = [d for d, ext in ATTENDUS.items()
             if not os.path.isdir(f"{CHEMIN}/{d}")
             or not any(f.endswith(ext) for f in os.listdir(f"{CHEMIN}/{d}"))]
if manquants:
    raise FileNotFoundError(
        f"Fichiers absents dans : {', '.join(manquants)}. Importer data/donnees_sfe_diabete.zip "
        f"à la racine du Volume (Catalog > workspace > {SCHEMA} > Volumes > brut), puis relancer.")
print("\n✅ Les 4 dossiers attendus contiennent des fichiers.")

# COMMAND ----------

import re
import unicodedata

def nom_colonne(texte):
    """'Libellé savoir-faire' -> 'libelle_savoir_faire' (Delta n'accepte ni espaces ni accents)."""
    texte = unicodedata.normalize("NFKD", str(texte)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", texte.lower()).strip("_")

def normaliser_colonnes(df):
    return df.toDF(*[nom_colonne(c) for c in df.columns])

def enregistrer(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"{table} : {spark.table(table).count():,} lignes".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Medic'AM (fichiers Excel .xls)
# MAGIC
# MAGIC Chaque fichier contient 3 onglets (tous prescripteurs / prescripteurs de ville / prescripteurs
# MAGIC hospitaliers). Chaque onglet a 9 colonnes descriptives puis, pour chacun des 6 mois,
# MAGIC 3 mesures : base de remboursement, nombre de boîtes, montant remboursé.
# MAGIC
# MAGIC Les lignes sans code CIP13 numérique (« Homéopathie », « Total », mentions d'édition)
# MAGIC sont exclues pour éviter les doubles comptes.

# COMMAND ----------

import glob
import os
import pandas as pd

ONGLETS = {"tous_presc": "TOUS", "presc_ville": "VILLE", "presc_hopital": "HOPITAL"}
COLONNES_PRODUIT = ["cip13", "nom_court", "produit", "code_ephmra", "classe_ephmra",
                    "code_atc", "classe_atc", "code_atc2", "libelle_atc2"]

def mesure_de(entete):
    texte = " ".join(str(entete).split()).lower()
    mois = re.search(r"\d{4}-\d{2}", texte).group(0)
    if texte.startswith("base"):
        return mois, "base_remboursement"
    if texte.startswith("nombre"):
        return mois, "nb_boites"
    return mois, "montant_rembourse"

def lire_medicam(fichier):
    blocs = []
    for onglet, df in pd.read_excel(fichier, sheet_name=None, engine="xlrd").items():
        prescripteur = next((v for k, v in ONGLETS.items() if onglet.endswith(k)), None)
        if prescripteur is None:      # onglet "lisez-moi"
            continue
        produit = df.iloc[:, :9].copy()
        produit.columns = COLONNES_PRODUIT
        cip = pd.to_numeric(produit["cip13"], errors="coerce")
        garder = cip.notna()
        produit = produit[garder].fillna("").astype(str)
        produit["cip13"] = cip[garder].astype("int64").astype(str)

        colonnes_par_mois = {}
        for entete in df.columns[9:]:
            mois, mesure = mesure_de(entete)
            colonnes_par_mois.setdefault(mois, {})[mesure] = entete

        for mois, colonnes in colonnes_par_mois.items():
            bloc = produit.copy()
            bloc["mois"] = mois
            bloc["prescripteur"] = prescripteur
            for mesure, entete in colonnes.items():
                bloc[mesure] = pd.to_numeric(df.loc[garder, entete], errors="coerce").fillna(0.0).astype(float)
            bloc["fichier_source"] = fichier.split("/")[-1]
            blocs.append(bloc)
    return pd.concat(blocs, ignore_index=True)

# Les fichiers sont importés compressés (.zip, plus légers) : on les décompresse dans le Volume
import zipfile
for archive in sorted(glob.glob(f"{CHEMIN}/medicam/*.zip")):
    with zipfile.ZipFile(archive) as z:
        for nom in z.namelist():
            if not os.path.exists(f"{CHEMIN}/medicam/{nom}"):
                z.extract(nom, f"{CHEMIN}/medicam/")

fichiers = sorted(glob.glob(f"{CHEMIN}/medicam/*.xls"))
print(f"{len(fichiers)} fichiers Medic'AM")
# Un fichier à la fois (≈ 270 000 lignes chacun) pour ne pas saturer la mémoire
for i, fichier in enumerate(fichiers):
    (spark.createDataFrame(lire_medicam(fichier)).write
     .mode("overwrite" if i == 0 else "append").option("overwriteSchema", "true")
     .saveAsTable("bronze_medicam"))
    print("  chargé :", fichier.split("/")[-1])
print(f"bronze_medicam : {spark.table('bronze_medicam').count():,} lignes".replace(",", " "))

# COMMAND ----------

# Contrôle : 60 mois attendus (2021-01 à 2025-12), 3 types de prescripteur chacun
display(spark.sql("""
    SELECT substr(mois, 1, 4) AS annee, count(DISTINCT mois) AS nb_mois,
           count(DISTINCT prescripteur) AS nb_prescripteurs, count(*) AS nb_lignes
    FROM bronze_medicam GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Base de données publique des médicaments (BDPM)
# MAGIC
# MAGIC Fichiers texte sans en-tête, séparés par des tabulations. Les noms de colonnes viennent
# MAGIC de la documentation officielle de la BDPM.

# COMMAND ----------

def lire_bdpm(fichier, colonnes, encodage):
    df = (spark.read.option("sep", "\t").option("header", "false")
          .option("encoding", encodage).option("quote", "")
          .csv(f"{CHEMIN}/bdpm/{fichier}"))
    return df.toDF(*colonnes[: len(df.columns)])

enregistrer(lire_bdpm("CIS_bdpm.txt", [
    "cis", "denomination", "forme_pharmaceutique", "voies_administration", "statut_amm",
    "type_procedure_amm", "etat_commercialisation", "date_amm", "statut_bdm",
    "numero_autorisation_europeenne", "titulaires", "surveillance_renforcee"], "ISO-8859-1"),
    "bronze_bdpm_specialites")

enregistrer(lire_bdpm("CIS_CIP_bdpm.txt", [
    "cis", "cip7", "libelle_presentation", "statut_administratif", "etat_commercialisation",
    "date_declaration_commercialisation", "cip13", "agrement_collectivites",
    "taux_remboursement", "prix_sans_honoraires", "prix_avec_honoraires", "honoraires",
    "indications_remboursement"], "UTF-8"),
    "bronze_bdpm_presentations")

enregistrer(lire_bdpm("CIS_GENER_bdpm.txt", [
    "id_groupe_generique", "libelle_groupe_generique", "cis", "type_generique", "numero_tri"],
    "ISO-8859-1"),
    "bronze_bdpm_generiques")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transparence Santé

# COMMAND ----------

transparence = (spark.read.option("header", "true").option("sep", ";")
                .option("multiLine", "true").option("escape", '"')
                .csv(f"{CHEMIN}/transparence/*.csv.gz"))
transparence = transparence.toDF(*[c.replace("﻿", "") for c in transparence.columns])
enregistrer(transparence, "bronze_transparence")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Annuaire Santé RPPS
# MAGIC
# MAGIC Fichier réduit par `scripts/preparer_donnees.py` : médecins uniquement, sans données
# MAGIC personnelles (ni nom, ni téléphone, ni e-mail, ni adresse).
# MAGIC
# MAGIC Séparateur « | ». On désactive la gestion des guillemets (certains libellés en contiennent)
# MAGIC pour ne pas décaler les colonnes.

# COMMAND ----------

rpps = (spark.read.option("header", "true").option("sep", "|").option("quote", "")
        .csv(f"{CHEMIN}/rpps/rpps_medecins.txt.gz"))
enregistrer(normaliser_colonnes(rpps), "bronze_rpps")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Récapitulatif

# COMMAND ----------

display(spark.sql("SHOW TABLES").filter("tableName LIKE 'bronze_%'"))
