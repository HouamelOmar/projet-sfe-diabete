# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Couche Gold : indicateurs métier
# MAGIC
# MAGIC Tables prêtes pour l'analyse et le tableau de bord. Un **cycle** = un **semestre**
# MAGIC (10 cycles de 2021-S1 à 2025-S2).
# MAGIC
# MAGIC | Table | Question métier | Grain |
# MAGIC |---|---|---|
# MAGIC | `gold_ventes_mensuelles` | Comment évolue le marché, produit par produit ? | produit × mois |
# MAGIC | `gold_parts_de_marche` | Qui gagne et qui perd des parts de marché ? | laboratoire × semestre |
# MAGIC | `gold_parts_de_marche_classe` | Qui domine chaque classe thérapeutique ? | classe × laboratoire × semestre |
# MAGIC | `gold_investissements` | Combien chaque labo investit auprès des médecins, et qui touche-t-il ? | laboratoire × semestre |
# MAGIC | `gold_investissements_detail` | Comment (type d'action), sur quel thème, auprès de qui (spécialité) et où (région) ? | labo × semestre × catégorie × thème × spécialité × région |
# MAGIC | `gold_impact` | L'investissement d'un cycle est-il suivi d'une hausse des ventes ? | laboratoire × semestre |
# MAGIC
# MAGIC **Évolutions** : calculées par rapport au **même semestre de l'année précédente**, pour neutraliser
# MAGIC la saisonnalité (le 2e semestre n'est pas comparable au 1er).

# COMMAND ----------

spark.sql("USE workspace.sfe_diabete")

def afficher_nb_lignes(table):
    print(f"{table} : {spark.table(table).count():,} lignes".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `gold_ventes_mensuelles`

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_ventes_mensuelles AS
SELECT
  date_mois, annee, semestre, trimestre,
  classe_therapeutique, groupe_laboratoire, type_produit, produit, molecule,
  bool_or(indication_hors_diabete_possible)                            AS indication_hors_diabete_possible,
  sum(CASE WHEN prescripteur = 'TOUS'    THEN ventes_eur END)          AS ventes_eur,
  sum(CASE WHEN prescripteur = 'VILLE'   THEN ventes_eur END)          AS ventes_prescripteurs_ville_eur,
  sum(CASE WHEN prescripteur = 'HOPITAL' THEN ventes_eur END)          AS ventes_prescripteurs_hopital_eur,
  sum(CASE WHEN prescripteur = 'TOUS'    THEN nb_boites END)           AS nb_boites
FROM silver_ventes_diabete
GROUP BY ALL
""")
afficher_nb_lignes("gold_ventes_mensuelles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `gold_parts_de_marche` (laboratoire × semestre)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_parts_de_marche AS
WITH ventes AS (
  SELECT semestre, groupe_laboratoire, sum(ventes_eur) AS ventes_eur, sum(nb_boites) AS nb_boites
  FROM gold_ventes_mensuelles
  GROUP BY ALL
),
parts AS (
  SELECT *, try_divide(ventes_eur, sum(ventes_eur) OVER (PARTITION BY semestre)) AS part_de_marche
  FROM ventes
)
SELECT
  p.semestre, p.groupe_laboratoire, p.ventes_eur, p.nb_boites, p.part_de_marche,
  try_divide(p.ventes_eur, n1.ventes_eur) - 1              AS evolution_ventes_annuelle,
  (p.part_de_marche - n1.part_de_marche) * 100             AS evolution_part_points
FROM parts p
LEFT JOIN parts n1
  ON  n1.groupe_laboratoire = p.groupe_laboratoire
  AND n1.semestre = concat(cast(cast(left(p.semestre, 4) AS INT) - 1 AS STRING), substr(p.semestre, 5))
""")
afficher_nb_lignes("gold_parts_de_marche")

# Contrôle : les parts de marché somment à 100 % chaque semestre
ecart = spark.sql("""
  SELECT max(abs(total - 1)) AS ecart FROM (
    SELECT semestre, sum(part_de_marche) AS total FROM gold_parts_de_marche GROUP BY semestre)
""").first().ecart
assert ecart < 1e-9, "Les parts de marché doivent sommer à 100 %"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `gold_parts_de_marche_classe` (classe thérapeutique × laboratoire × semestre)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_parts_de_marche_classe AS
WITH ventes AS (
  SELECT semestre, classe_therapeutique, groupe_laboratoire, sum(ventes_eur) AS ventes_eur
  FROM gold_ventes_mensuelles
  GROUP BY ALL
)
SELECT *,
  try_divide(ventes_eur, sum(ventes_eur) OVER (PARTITION BY semestre, classe_therapeutique)) AS part_dans_la_classe,
  sum(ventes_eur) OVER (PARTITION BY semestre, classe_therapeutique)
    / sum(ventes_eur) OVER (PARTITION BY semestre)                               AS poids_de_la_classe
FROM ventes
""")
afficher_nb_lignes("gold_parts_de_marche_classe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `gold_investissements` (laboratoire × semestre)
# MAGIC
# MAGIC - **Investissement total** : avantages + rémunérations versés à des médecins (toutes aires thérapeutiques).
# MAGIC - **Investissement diabète (estimé)** : montants liés à un événement sur le diabète (quelle que soit la
# MAGIC   spécialité du médecin), ou versés à des endocrinologues-diabétologues quand l'événement n'est pas précisé
# MAGIC   (règle détaillée dans le notebook 03). C'est une **estimation basse** : une partie des repas et
# MAGIC   invitations n'indique pas l'événement.
# MAGIC - **Investissement obésité** et **cardio-rénal** : présentés à part. Le cardio-rénal éclaire
# MAGIC   AstraZeneca et Boehringer Ingelheim, dont les gliflozines sont aussi prescrites dans l'insuffisance
# MAGIC   cardiaque et rénale.
# MAGIC - **Couverture des endocrinologues** : part des endocrinologues **en activité** touchés au moins une fois
# MAGIC   dans le semestre (indicateur SFE classique de couverture de cible).

# COMMAND ----------

nb_endocrinologues = spark.sql("""
    SELECT count(*) AS n FROM silver_medecins
    WHERE specialite = 'Endocrinologie-diabétologie' AND en_activite
""").first().n
print(f"Endocrinologues-diabétologues en activité dans le RPPS : {nb_endocrinologues}")

spark.sql(f"""
CREATE OR REPLACE TABLE gold_investissements AS
SELECT
  semestre, groupe_laboratoire,
  sum(montant_verse)                                                                  AS invest_total_eur,
  sum(CASE WHEN cible_diabete THEN montant_verse ELSE 0 END)                          AS invest_diabete_eur,
  sum(CASE WHEN theme_evenement = 'Obésité' THEN montant_verse ELSE 0 END)            AS invest_obesite_eur,
  sum(CASE WHEN theme_evenement = 'Cardio-rénal' THEN montant_verse ELSE 0 END)       AS invest_cardio_renal_eur,
  sum(CASE WHEN specialite = 'Endocrinologie-diabétologie' THEN montant_verse ELSE 0 END) AS invest_endocrinologues_eur,
  sum(CASE WHEN specialite = 'Médecine générale' THEN montant_verse ELSE 0 END)       AS invest_generalistes_eur,
  count(*)                                                                            AS nb_declarations,
  count_if(cible_diabete)                                                             AS nb_actions_diabete,
  count_if(type_lien = 'convention')                                                  AS nb_conventions,
  count(DISTINCT rpps)                                                                AS nb_medecins_touches,
  count(DISTINCT CASE WHEN specialite = 'Endocrinologie-diabétologie' THEN rpps END)  AS nb_endocrinologues_touches,
  count(DISTINCT CASE WHEN specialite = 'Médecine générale' THEN rpps END)            AS nb_generalistes_touches,
  count(DISTINCT CASE WHEN specialite = 'Endocrinologie-diabétologie' THEN rpps END)
    / {nb_endocrinologues}                                                            AS couverture_endocrinologues,
  semestre = '2021-S1'                                                                AS periode_covid
FROM silver_declarations
GROUP BY semestre, groupe_laboratoire
""")
afficher_nb_lignes("gold_investissements")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `gold_investissements_detail` (pour les filtres du tableau de bord)
# MAGIC
# MAGIC ⚠️ `nb_medecins` est un nombre de médecins **distincts** : il ne s'additionne pas d'une ligne à l'autre
# MAGIC (un médecin peut apparaître dans plusieurs catégories). Les montants, eux, s'additionnent.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_investissements_detail AS
SELECT
  annee, semestre, groupe_laboratoire, categorie_action, theme_evenement, specialite, region, cible_diabete,
  sum(montant_verse)     AS montant_verse_eur,
  count(*)               AS nb_declarations,
  count(DISTINCT rpps)   AS nb_medecins
FROM silver_declarations
GROUP BY ALL
""")
afficher_nb_lignes("gold_investissements_detail")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `gold_impact` : investissement d'un cycle et ventes du cycle suivant
# MAGIC
# MAGIC Pour chacun des 7 laboratoires suivis dans Transparence Santé, on met côte à côte :
# MAGIC - l'investissement du semestre *t* et son évolution sur un an ;
# MAGIC - les ventes du semestre *t* et du semestre *t + 1* (effet décalé : une action menée aujourd'hui
# MAGIC   influence les prescriptions des mois suivants) ;
# MAGIC - l'**intensité promotionnelle** : euros investis (diabète) pour 1 000 € de ventes.
# MAGIC
# MAGIC ⚠️ Ces rapprochements sont **descriptifs** : avec 7 laboratoires × 10 semestres, et des ventes portées
# MAGIC par d'autres facteurs (nouvelles indications, génériques, pénuries), ils ne prouvent pas un effet causal.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_impact AS
WITH base AS (
  SELECT
    i.semestre, i.groupe_laboratoire,
    i.invest_total_eur, i.invest_diabete_eur, i.nb_actions_diabete,
    i.nb_endocrinologues_touches, i.couverture_endocrinologues, i.periode_covid,
    v.ventes_eur, v.part_de_marche, v.evolution_ventes_annuelle, v.evolution_part_points
  FROM gold_investissements i
  JOIN gold_parts_de_marche v USING (semestre, groupe_laboratoire)
)
SELECT
  *,
  1000 * try_divide(invest_diabete_eur, ventes_eur)                               AS intensite_promo_pour_1000_eur,
  try_divide(invest_diabete_eur,
             lag(invest_diabete_eur, 2) OVER (PARTITION BY groupe_laboratoire ORDER BY semestre)) - 1
                                                                                  AS evolution_invest_diabete_annuelle,
  lead(ventes_eur) OVER (PARTITION BY groupe_laboratoire ORDER BY semestre)       AS ventes_semestre_suivant_eur,
  lead(evolution_ventes_annuelle) OVER (PARTITION BY groupe_laboratoire ORDER BY semestre)
                                                                                  AS evolution_ventes_semestre_suivant,
  lead(evolution_part_points) OVER (PARTITION BY groupe_laboratoire ORDER BY semestre)
                                                                                  AS evolution_part_semestre_suivant
FROM base
""")
afficher_nb_lignes("gold_impact")

# Contrôle : 7 laboratoires × 10 semestres
dimensions = spark.sql("""
  SELECT count(DISTINCT groupe_laboratoire) AS labos, count(DISTINCT semestre) AS semestres FROM gold_impact
""").first()
print(dimensions)
assert dimensions.labos == 7 and dimensions.semestres == 10, "7 laboratoires × 10 semestres attendus"

# COMMAND ----------

display(spark.sql("""
  SELECT semestre, groupe_laboratoire,
         round(ventes_eur / 1e6, 1)                  AS ventes_meur,
         round(100 * part_de_marche, 1)              AS part_pct,
         round(invest_diabete_eur / 1e3, 0)          AS invest_diabete_keur,
         round(intensite_promo_pour_1000_eur, 2)     AS eur_investis_pour_1000_eur_ventes,
         round(100 * couverture_endocrinologues, 1)  AS couverture_endocrino_pct
  FROM gold_impact
  WHERE semestre = '2025-S2'
  ORDER BY ventes_eur DESC
"""))
