# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Couche Silver : nettoyage, enrichissement et contrôles qualité
# MAGIC
# MAGIC | Table créée | Contenu | Grain |
# MAGIC |---|---|---|
# MAGIC | `silver_ventes_diabete` | Ventes des antidiabétiques (ATC A10) avec laboratoire, classe thérapeutique, marque / générique | médicament × mois × type de prescripteur |
# MAGIC | `silver_medecins` | Un médecin = une ligne, avec sa spécialité regroupée et son mode d'exercice | médecin (RPPS) |
# MAGIC | `silver_declarations` | Déclarations Transparence Santé typées (catégorie d'action, montant réellement versé, spécialité et région du médecin) | déclaration |
# MAGIC
# MAGIC **Règles méthodologiques importantes** (détaillées dans chaque section) :
# MAGIC 1. Ventes = *base de remboursement* Medic'AM (≈ chiffre d'affaires au prix public), pharmacies de ville uniquement.
# MAGIC 2. Le laboratoire d'un médicament est son **titulaire d'AMM** (BDPM), regroupé par groupe pharmaceutique.
# MAGIC 3. Transparence Santé : on additionne uniquement les **avantages et rémunérations** (sommes réellement versées) ;
# MAGIC    les **conventions** sont comptées mais pas additionnées (montants prévus, souvent redéclarés ensuite → double compte).
# MAGIC 4. Aucune donnée nominative : les médecins sont identifiés uniquement par leur numéro RPPS.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE workspace.sfe_diabete")

def sans_accents(colonne):
    """Minuscules sans accents, pour des comparaisons de texte robustes."""
    return F.translate(F.lower(colonne), "éèêëàâäîïôöûüùç’", "eeeeaaaiioouuuc'")

def enregistrer(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"{table} : {spark.table(table).count():,} lignes".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Référentiel laboratoires et médicaments
# MAGIC
# MAGIC **Pièges traités :**
# MAGIC - « MERCK SANTÉ » (Glucophage) appartient au groupe allemand **Merck KGaA**, à ne pas confondre avec **MSD** (Merck & Co.).
# MAGIC - **Biogaran** est la filiale génériques du groupe **Servier**.
# MAGIC - Plusieurs médicaments MSD (Januvia, Janumet, Xelevia, Velmetia) ne figurent plus dans la BDPM :
# MAGIC   ils sont rattachés manuellement à leur titulaire (table de correspondance ci-dessous).

# COMMAND ----------

# (motif recherché dans le nom du titulaire, groupe pharmaceutique) - l'ordre compte
GROUPES_LABORATOIRES = [
    ("novo nordisk", "NOVO NORDISK"),
    ("lilly", "LILLY"),
    ("astrazeneca", "ASTRAZENECA"),
    ("sanofi", "SANOFI"),
    ("merck sharp", "MSD"),
    ("msd", "MSD"),
    ("boehringer", "BOEHRINGER INGELHEIM"),
    ("servier", "SERVIER"),
    ("biogaran", "SERVIER"),
    ("merck sante", "MERCK KGAA"),
    ("merck serono", "MERCK KGAA"),
    ("novartis", "NOVARTIS"),
    ("janssen", "JOHNSON & JOHNSON"),
    ("viatris", "VIATRIS"),
    ("mylan", "VIATRIS"),
    ("sandoz", "SANDOZ"),
    ("teva", "TEVA"),
]

def groupe_laboratoire(colonne_titulaire):
    titulaire = sans_accents(colonne_titulaire)
    expression = F.lit("AUTRES LABORATOIRES")
    for motif, groupe in reversed(GROUPES_LABORATOIRES):
        expression = F.when(titulaire.contains(motif), F.lit(groupe)).otherwise(expression)
    return F.when(colonne_titulaire.isNull(), F.lit("NON RÉFÉRENCÉ")).otherwise(expression)

# Médicaments absents de la BDPM : rattachement manuel au titulaire d'AMM
CORRESPONDANCE_MANUELLE = spark.createDataFrame([
    ("JANUVIA", "MERCK SHARP & DOHME (hors BDPM)", "Marque"),
    ("JANUMET", "MERCK SHARP & DOHME (hors BDPM)", "Marque"),
    ("XELEVIA", "MERCK SHARP & DOHME (hors BDPM)", "Marque"),
    ("VELMETIA", "MERCK SHARP & DOHME (hors BDPM)", "Marque"),
    ("ABASAGLAR", "ELI LILLY (hors BDPM)", "Marque"),
], ["produit_manuel", "titulaire_manuel", "type_manuel"])

# Présentation (CIP13) -> médicament (CIS) -> titulaire + statut générique
specialites = (spark.table("bronze_bdpm_specialites")
               .select("cis", F.trim(F.split("titulaires", ";")[0]).alias("titulaire")))
generiques = (spark.table("bronze_bdpm_generiques")
              .groupBy("cis")
              .agg(F.min("type_generique").alias("type_generique")))   # 0 = princeps
referentiel = (spark.table("bronze_bdpm_presentations").select("cip13", "cis")
               .join(specialites, "cis", "left")
               .join(generiques, "cis", "left")
               .withColumn("type_produit",
                           F.when(F.col("type_generique").isin("1", "2", "4"), "Générique")
                            .otherwise("Marque"))
               .dropDuplicates(["cip13"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Classes thérapeutiques (classification ATC officielle de l'OMS)
# MAGIC
# MAGIC Les associations fixes (ex. Janumet = sitagliptine + metformine, Synjardy = empagliflozine + metformine)
# MAGIC sont rattachées à la classe de leur molécule innovante, car c'est la logique concurrentielle des laboratoires.

# COMMAND ----------

ASSOCIATIONS_DPP4 = ["A10BD07", "A10BD08", "A10BD10", "A10BD11", "A10BD13", "A10BD24"]
ASSOCIATIONS_SGLT2 = ["A10BD15", "A10BD16", "A10BD20", "A10BD23", "A10BD25"]

def classe_therapeutique(atc):
    return (F.when(atc.startswith("A10A"), "Insulines")
             .when(atc.startswith("A10BA"), "Metformine")
             .when(atc.startswith("A10BB"), "Sulfamides hypoglycémiants")
             .when(atc.startswith("A10BH") | atc.isin(ASSOCIATIONS_DPP4), "Inhibiteurs DPP-4 (gliptines)")
             .when(atc.startswith("A10BK") | atc.isin(ASSOCIATIONS_SGLT2), "Inhibiteurs SGLT2 (gliflozines)")
             .when(atc.startswith("A10BJ") | (atc == "A10BX16"), "Analogues du GLP-1")
             .when(atc.startswith("A10BF"), "Inhibiteurs des alpha-glucosidases")
             .when(atc.startswith("A10BD"), "Autres associations")
             .otherwise("Autres antidiabétiques"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `silver_ventes_diabete`

# COMMAND ----------

ventes = (spark.table("bronze_medicam")
          .filter(F.col("code_atc").startswith("A10"))
          .join(referentiel, "cip13", "left")
          .join(CORRESPONDANCE_MANUELLE,
                F.upper(F.col("produit")) == F.col("produit_manuel"), "left")
          .withColumn("titulaire", F.coalesce("titulaire", "titulaire_manuel"))
          .withColumn("type_produit",
                      F.coalesce(F.when(F.col("cis").isNotNull(), F.col("type_produit")),
                                 F.col("type_manuel"),
                                 F.lit("Non déterminé")))
          .withColumn("date_mois", F.to_date(F.concat("mois", F.lit("-01"))))
          .select(
              "date_mois",
              F.year("date_mois").alias("annee"),
              F.concat(F.year("date_mois"), F.lit("-S"),
                       F.when(F.month("date_mois") <= 6, 1).otherwise(2)).alias("semestre"),
              F.concat(F.year("date_mois"), F.lit("-T"), F.quarter("date_mois")).alias("trimestre"),
              "prescripteur", "cip13", "nom_court", "produit",
              F.col("code_atc"), F.col("classe_atc").alias("molecule"),
              classe_therapeutique(F.col("code_atc")).alias("classe_therapeutique"),
              # dapagliflozine (Forxiga) et empagliflozine (Jardiance) sont aussi prescrites
              # dans l'insuffisance cardiaque et rénale : une partie des ventes est hors diabète
              F.col("code_atc").isin("A10BK01", "A10BK03", "A10BD15", "A10BD20")
               .alias("indication_hors_diabete_possible"),
              "titulaire",
              groupe_laboratoire(F.col("titulaire")).alias("groupe_laboratoire"),
              "type_produit",
              F.col("base_remboursement").alias("ventes_eur"),
              "nb_boites",
              "montant_rembourse",
          ))
enregistrer(ventes, "silver_ventes_diabete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contrôles qualité des ventes

# COMMAND ----------

controle = spark.sql("""
    SELECT
      count(DISTINCT date_mois)                                             AS nb_mois,
      round(sum(ventes_eur) / 1e9, 2)                                       AS ventes_totales_mds_eur,
      round(100 * sum(CASE WHEN groupe_laboratoire <> 'NON RÉFÉRENCÉ' THEN ventes_eur END)
            / sum(ventes_eur), 2)                                           AS pct_ventes_avec_laboratoire,
      sum(CASE WHEN ventes_eur < 0 THEN 1 ELSE 0 END)                       AS nb_ventes_negatives
    FROM silver_ventes_diabete
    WHERE prescripteur = 'TOUS'
""").first()
print(controle)
assert controle.nb_mois == 60, "60 mois attendus (2021-01 à 2025-12)"
assert controle.pct_ventes_avec_laboratoire >= 99, "Moins de 99 % des ventes rattachées à un laboratoire"

# Ville + hôpital doit être égal au total, mois par mois
ecart = spark.sql("""
    SELECT max(abs(tous - ville_hopital)) AS ecart_max FROM (
      SELECT date_mois,
             sum(CASE WHEN prescripteur = 'TOUS' THEN ventes_eur END) AS tous,
             sum(CASE WHEN prescripteur <> 'TOUS' THEN ventes_eur END) AS ville_hopital
      FROM silver_ventes_diabete GROUP BY date_mois)
""").first().ecart_max
print(f"Écart maximal ville + hôpital vs total : {ecart:.2f} €")
assert ecart < 1, "Incohérence entre les onglets Medic'AM"

# COMMAND ----------

display(spark.sql("""
    SELECT groupe_laboratoire, round(sum(ventes_eur) / 1e6, 1) AS ventes_2025_meur
    FROM silver_ventes_diabete
    WHERE prescripteur = 'TOUS' AND annee = 2025
    GROUP BY 1 ORDER BY 2 DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver_medecins`
# MAGIC
# MAGIC Un médecin peut avoir plusieurs activités (lignes). On garde **une seule spécialité** par médecin,
# MAGIC par ordre de priorité pour l'étude : endocrinologie > cardiologie > néphrologie > médecine générale > autres.

# COMMAND ----------

libelle = sans_accents(F.col("libelle_savoir_faire"))
specialite = (F.when(libelle.contains("endocrinologie"), "Endocrinologie-diabétologie")
               .when(libelle.contains("cardiologie"), "Cardiologie")
               .when(libelle.contains("nephrologie"), "Néphrologie")
               .when(libelle.contains("medecine generale"), "Médecine générale")
               .when(F.col("libelle_savoir_faire").isNotNull(), "Autres spécialités")
               .otherwise("Non renseignée"))
priorite = (F.when(specialite == "Endocrinologie-diabétologie", 1).when(specialite == "Cardiologie", 2)
             .when(specialite == "Néphrologie", 3).when(specialite == "Médecine générale", 4)
             .when(specialite == "Autres spécialités", 5).otherwise(6))

activites = (spark.table("bronze_rpps")
             .filter(F.col("code_profession") == "10")
             .select(F.col("identifiant_pp").alias("rpps"),
                     specialite.alias("specialite"),
                     priorite.alias("priorite"),
                     F.col("libelle_savoir_faire").alias("specialite_detaillee"),
                     F.col("code_mode_exercice").alias("mode_exercice")))

modes = (activites.groupBy("rpps")
         .agg(F.collect_set("mode_exercice").alias("modes"))
         .withColumn("mode_exercice",
                     F.when(F.array_contains("modes", "L") & F.array_contains("modes", "S"), "Mixte")
                      .when(F.array_contains("modes", "L"), "Libéral")
                      .when(F.array_contains("modes", "S"), "Salarié")
                      .otherwise("Non renseigné"))
         .drop("modes"))

medecins = (activites
            .withColumn("rang", F.row_number().over(
                Window.partitionBy("rpps").orderBy("priorite", "specialite_detaillee")))
            .filter("rang = 1")
            .select("rpps", "specialite", "specialite_detaillee")
            .join(modes, "rpps"))
enregistrer(medecins, "silver_medecins")

nb_doublons = spark.sql("SELECT count(*) - count(DISTINCT rpps) AS n FROM silver_medecins").first().n
assert nb_doublons == 0, "Un médecin doit apparaître une seule fois"
display(spark.sql("SELECT specialite, count(*) AS nb_medecins FROM silver_medecins GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `silver_declarations` (Transparence Santé)
# MAGIC
# MAGIC **Catégories d'action** (regroupement des motifs déclarés) :
# MAGIC
# MAGIC | Catégorie | Motifs d'origine |
# MAGIC |---|---|
# MAGIC | Rémunérations | paiements liés à une convention (orateur, expert, consultant…) |
# MAGIC | Contrats orateurs et experts | contrats d'intervenant, d'expert scientifique, de conseil… |
# MAGIC | Repas | hospitalité - restauration, collations |
# MAGIC | Congrès et événements | inscription, transport, hébergement, participation à une manifestation |
# MAGIC | Formation | formation |
# MAGIC | Recherche | contrats et bourses de recherche |
# MAGIC
# MAGIC **Ciblage diabète** : une déclaration est considérée comme liée au diabète si le médecin est
# MAGIC endocrinologue-diabétologue **ou** si l'événement mentionne le diabète (« diab », congrès SFD, EASD).
# MAGIC Transparence Santé ne précise pas le produit promu : c'est une **estimation**, assumée comme telle.

# COMMAND ----------

REGIONS = {
    "Auvergne-Rhône-Alpes": ["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"],
    "Bourgogne-Franche-Comté": ["21", "25", "39", "58", "70", "71", "89", "90"],
    "Bretagne": ["22", "29", "35", "56"],
    "Centre-Val de Loire": ["18", "28", "36", "37", "41", "45"],
    "Corse": ["2A", "2B"],
    "Grand Est": ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"],
    "Hauts-de-France": ["02", "59", "60", "62", "80"],
    "Île-de-France": ["75", "77", "78", "91", "92", "93", "94", "95"],
    "Normandie": ["14", "27", "50", "61", "76"],
    "Nouvelle-Aquitaine": ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"],
    "Occitanie": ["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"],
    "Pays de la Loire": ["44", "49", "53", "72", "85"],
    "Provence-Alpes-Côte d'Azur": ["04", "05", "06", "13", "83", "84"],
    "Outre-mer": ["971", "972", "973", "974", "976"],
}
departements_regions = spark.createDataFrame(
    [(d, r) for r, liste in REGIONS.items() for d in liste], ["departement", "region"])

GROUPES_TRANSPARENCE = {
    "NOVO NORDISK": "NOVO NORDISK",
    "SANOFI WINTHROP INDUSTRIE": "SANOFI", "SANOFI AVENTIS FRANCE": "SANOFI",
    "LILLY FRANCE": "LILLY", "LILLY FRANCE SAS": "LILLY",
    "ASTRAZENECA": "ASTRAZENECA",
    "BOEHRINGER INGELHEIM FRANCE": "BOEHRINGER INGELHEIM",
    "MSD FRANCE": "MSD",
    "LES LABORATOIRES SERVIER": "SERVIER",
}
groupes_transparence = spark.createDataFrame(list(GROUPES_TRANSPARENCE.items()),
                                             ["raison_sociale", "groupe_laboratoire"])

motif = sans_accents(F.col("motif_lien_interet"))
categorie = (F.when(F.col("lien_interet") == "remuneration", "Rémunérations")
              .when(motif.rlike("intervenant|expert|conseil|interview|evaluation"), "Contrats orateurs et experts")
              .when(motif.rlike("recherche|bourse"), "Recherche")
              .when(motif.rlike("restauration|collation"), "Repas")
              .when(motif.rlike("hebergement|transport|inscription|manifestation|hospitalite"), "Congrès et événements")
              .when(motif.contains("formation"), "Formation")
              .otherwise("Autres"))

identifiant = F.trim(F.col("beneficiaire_identifiant"))
rpps = (F.when(identifiant.rlike(r"^\d{11}$"), identifiant)
         .when(identifiant.rlike(r"^8\d{11}$"), identifiant.substr(2, 11)))  # identification nationale = 8 + RPPS

code_postal = F.lpad(F.trim(F.col("code_postal")), 5, "0")
departement = (F.when(code_postal.startswith("97"), code_postal.substr(1, 3))
                .when(code_postal.startswith("20"),
                      F.when(code_postal < "20200", "2A").otherwise("2B"))
                .otherwise(code_postal.substr(1, 2)))

texte_evenement = F.concat_ws(" ", "information_evenement", "autre_motif")

declarations = (spark.table("bronze_transparence")
    .dropDuplicates(["id"])
    .join(groupes_transparence, "raison_sociale")
    .withColumn("date", F.to_date(F.substring("date", 1, 10)))
    .withColumn("montant", F.col("montant").cast("double"))
    .withColumn("rpps", rpps)
    .withColumn("departement", departement)
    .join(spark.table("silver_medecins").select("rpps", "specialite", "mode_exercice"), "rpps", "left")
    .join(departements_regions, "departement", "left")
    .select(
        "id", "date",
        F.year("date").alias("annee"),
        F.concat(F.year("date"), F.lit("-S"), F.when(F.month("date") <= 6, 1).otherwise(2)).alias("semestre"),
        F.concat(F.year("date"), F.lit("-T"), F.quarter("date")).alias("trimestre"),
        "groupe_laboratoire",
        F.col("raison_sociale").alias("entreprise_declarante"),
        F.col("lien_interet").alias("type_lien"),
        F.col("motif_lien_interet").alias("motif"),
        categorie.alias("categorie_action"),
        F.coalesce("montant", F.lit(0.0)).alias("montant_declare"),
        # sommes réellement versées : avantages + rémunérations (pas les conventions, cf. règle 3)
        F.when(F.col("lien_interet").isin("avantage", "remuneration"),
               F.coalesce("montant", F.lit(0.0))).otherwise(0.0).alias("montant_verse"),
        "rpps",
        F.coalesce("specialite", F.lit("Non identifiée")).alias("specialite"),
        F.coalesce("mode_exercice", F.lit("Non identifié")).alias("mode_exercice"),
        "departement",
        F.coalesce("region", F.lit("Hors France / inconnue")).alias("region"),
        texte_evenement.rlike(r"(?i)diab|\bSFD\b|EASD").alias("evenement_diabete"),
        "information_evenement",
    )
    .withColumn("cible_diabete",
                (F.col("specialite") == "Endocrinologie-diabétologie") | F.col("evenement_diabete")))
enregistrer(declarations, "silver_declarations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contrôles qualité des déclarations

# COMMAND ----------

display(spark.sql("""
    SELECT
      count(*)                                                               AS nb_declarations,
      count(DISTINCT groupe_laboratoire)                                     AS nb_laboratoires,
      round(100 * avg(CASE WHEN rpps IS NOT NULL THEN 1 ELSE 0 END), 1)      AS pct_avec_rpps,
      round(100 * avg(CASE WHEN specialite <> 'Non identifiée' THEN 1 ELSE 0 END), 1) AS pct_specialite_trouvee,
      round(100 * avg(CASE WHEN region <> 'Hors France / inconnue' THEN 1 ELSE 0 END), 1) AS pct_region_trouvee,
      round(sum(montant_verse) / 1e6, 1)                                     AS montant_verse_meur,
      sum(CASE WHEN montant_declare < 0 THEN 1 ELSE 0 END)                   AS nb_montants_negatifs
    FROM silver_declarations
"""))

# COMMAND ----------

# Volume mensuel : le 1er semestre 2021 est très faible (Covid : congrès annulés, restaurants fermés
# jusqu'au 9 juin 2021). Ce n'est pas une erreur de données, mais 2021 sera interprétée à part.
display(spark.sql("""
    SELECT annee, groupe_laboratoire, count(*) AS nb_declarations,
           round(sum(montant_verse) / 1e6, 2) AS montant_verse_meur,
           round(sum(CASE WHEN cible_diabete THEN montant_verse END) / 1e6, 2) AS montant_cible_diabete_meur
    FROM silver_declarations GROUP BY 1, 2 ORDER BY 1, 2
"""))
