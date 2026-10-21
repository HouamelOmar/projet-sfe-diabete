# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Analyse : dynamique concurrentielle du marché du diabète (2021-2025)
# MAGIC
# MAGIC Ce notebook répond aux questions de l'étude à partir des tables Gold (notebook 04).
# MAGIC Chaque section donne un graphique ou un tableau, puis une **lecture** (chiffres calculés ici même).
# MAGIC
# MAGIC | # | Question | Tables utilisées |
# MAGIC |---|---|---|
# MAGIC | 1 | Comment évolue le marché, et quelles classes le portent ? | `gold_ventes_mensuelles` |
# MAGIC | 2 | Qui gagne et qui perd des parts de marché, avec quels produits ? | `gold_parts_de_marche`, `gold_ventes_mensuelles` |
# MAGIC | 3 | Qui domine chaque classe thérapeutique ? | `gold_parts_de_marche_classe` |
# MAGIC | 4 | Combien et comment les laboratoires investissent-ils auprès des médecins ? | `gold_investissements`, `gold_investissements_detail` |
# MAGIC | 5 | Qui cible le mieux les endocrinologues, et avec quelle intensité ? | `gold_impact` |
# MAGIC | 6 | L'investissement d'un semestre est-il suivi d'une hausse des ventes ? | `gold_impact` |
# MAGIC | 7 | Synthèse : positionnement concurrentiel des 7 laboratoires | toutes |
# MAGIC
# MAGIC **Rappels de méthode** (détaillés dans les notebooks 03 et 04) : ventes = base de remboursement Medic'AM
# MAGIC (pharmacies de ville) ; investissement = avantages + rémunérations versés aux médecins (Transparence Santé) ;
# MAGIC 2021 est une année atypique (Covid : congrès et repas suspendus au 1er semestre).

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

spark.sql("USE workspace.sfe_diabete")

# Une couleur fixe par laboratoire, identique dans tous les graphiques (et dans le tableau de bord)
COULEURS = {
    "NOVO NORDISK": "#2a78d6",
    "ASTRAZENECA": "#eb6834",
    "LILLY": "#1baf7a",
    "BOEHRINGER INGELHEIM": "#eda100",
    "SANOFI": "#e87ba4",
    "SERVIER": "#008300",
    "MSD": "#6250d6",
}
LABOS = list(COULEURS)
NOMS = {"NOVO NORDISK": "Novo Nordisk", "ASTRAZENECA": "AstraZeneca", "LILLY": "Lilly",
        "BOEHRINGER INGELHEIM": "Boehringer Ingelheim", "SANOFI": "Sanofi", "SERVIER": "Servier", "MSD": "MSD"}
AUTRES = "#a3a29c"        # gris neutre : « autres laboratoires »
HAUSSE, BAISSE = "#2a78d6", "#e34948"
ENCRE, ENCRE_2, ENCRE_3, GRILLE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"

plt.rcParams.update({
    "figure.figsize": (10, 5), "figure.dpi": 110, "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb",
    "font.size": 10, "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.titlecolor": ENCRE, "axes.labelcolor": ENCRE_2, "xtick.color": ENCRE_3, "ytick.color": ENCRE_3,
    "axes.edgecolor": "#c3c2b7", "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRILLE, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "lines.linewidth": 2, "legend.frameon": False,
})

def sql(requete):
    return spark.sql(requete).toPandas()

def sous_titre(ax, texte):
    ax.text(0, 1.02, texte, transform=ax.transAxes, color=ENCRE_2, fontsize=9.5, va="bottom")

def etiquettes_fin_de_courbe(ax, df, x_fin, ecart_min, valeur_en_texte=lambda v: f"{v:.1f}"):
    """Nom (et dernière valeur) de chaque série au bout de sa courbe, écartés pour ne pas se chevaucher."""
    points = sorted(((df[c].iloc[-1], c) for c in df.columns), reverse=True)
    positions = []
    for valeur, nom in points:
        y = min(valeur, positions[-1] - ecart_min) if positions else valeur
        positions.append(y)
        texte = f"{NOMS.get(nom, nom)}  {valeur_en_texte(valeur)}" if valeur_en_texte else NOMS.get(nom, nom)
        ax.annotate(texte, xy=(x_fin, valeur), xytext=(x_fin + 0.15, y),
                    color=ENCRE_2, fontsize=9, va="center", annotation_clip=False)

def euros(x, _=None):
    return f"{x / 1e6:.0f} M€" if abs(x) >= 1e6 else f"{x / 1e3:.0f} k€"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Le marché : taille, croissance et classes thérapeutiques

# COMMAND ----------

marche = sql("""
    SELECT annee, sum(ventes_eur) AS ventes_eur, sum(nb_boites) AS nb_boites
    FROM gold_ventes_mensuelles GROUP BY annee ORDER BY annee
""")
marche["croissance_pct"] = 100 * marche["ventes_eur"].pct_change()
marche["prix_moyen_boite_eur"] = marche["ventes_eur"] / marche["nb_boites"]
display(marche.round(1))

# Les petites classes sont regroupées pour garder un graphique lisible
GRANDES_CLASSES = ["Analogues du GLP-1", "Inhibiteurs SGLT2 (gliflozines)", "Insulines",
                   "Inhibiteurs DPP-4 (gliptines)", "Metformine"]
classes = sql("SELECT annee, classe_therapeutique, sum(ventes_eur) AS ventes_eur FROM gold_ventes_mensuelles GROUP BY ALL")
classes["classe"] = classes["classe_therapeutique"].where(classes["classe_therapeutique"].isin(GRANDES_CLASSES),
                                                          "Autres classes")
classes = classes.pivot_table(index="annee", columns="classe", values="ventes_eur", aggfunc="sum")

fig, ax = plt.subplots()
classes = classes[GRANDES_CLASSES + ["Autres classes"]]
for i, classe in enumerate(classes.columns):
    couleur = list(COULEURS.values())[i] if classe != "Autres classes" else AUTRES
    ax.plot(classes.index, classes[classe], color=couleur, marker="o", markersize=5)
etiquettes_fin_de_courbe(ax, classes, classes.index[-1], ecart_min=30e6, valeur_en_texte=None)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(euros))
ax.set_xticks(classes.index)
ax.set_xlim(classes.index[0] - 0.2, classes.index[-1] + 1.4)
ax.set_title("Ventes annuelles par classe thérapeutique", pad=22)
sous_titre(ax, "Base de remboursement Medic'AM, pharmacies de ville, tous prescripteurs")
plt.show()

# COMMAND ----------

premiere, derniere = marche.iloc[0], marche.iloc[-1]
tcam = (derniere.ventes_eur / premiere.ventes_eur) ** (1 / (len(marche) - 1)) - 1
c = classes.iloc[[0, -1]].T / 1e6
print(f"Marché : {premiere.ventes_eur / 1e9:.2f} Md€ en {premiere.annee:.0f} -> {derniere.ventes_eur / 1e9:.2f} Md€ "
      f"en {derniere.annee:.0f} (croissance annuelle moyenne {100 * tcam:.1f} %).")
print(f"Boîtes : +{100 * (derniere.nb_boites / premiere.nb_boites - 1):.0f} % ; prix moyen par boîte : "
      f"{premiere.prix_moyen_boite_eur:.1f} € -> {derniere.prix_moyen_boite_eur:.1f} € (déplacement vers des produits plus chers).")
for classe, (debut, fin) in c.iterrows():
    print(f"  {classe:35s} {debut:6.0f} M€ -> {fin:6.0f} M€  ({fin - debut:+.0f} M€)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Lecture.** Le marché passe de 1,5 à 2,1 Md€ entre 2021 et 2025 (+9,5 % par an, +44 % au total), mais
# MAGIC les volumes ne progressent que de 26 % : la croissance vient aussi du **remplacement de traitements anciens et
# MAGIC peu chers par des innovations coûteuses** (prix moyen par boîte de 20,6 € à 23,5 €).
# MAGIC Deux classes portent toute la croissance : les **gliflozines (SGLT2)**, passées de quasi rien à plus de
# MAGIC 600 M€, et les **analogues du GLP-1**. À l'inverse, les **gliptines (DPP-4)** s'effondrent avec l'arrivée
# MAGIC des génériques de la sitagliptine, et les **insulines** reculent lentement.
# MAGIC
# MAGIC ⚠️ Une partie des gliflozines (Forxiga, Jardiance) est prescrite dans l'insuffisance cardiaque et
# MAGIC rénale : leur croissance ne relève pas uniquement du diabète.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parts de marché : qui gagne, qui perd ?

# COMMAND ----------

parts = sql(f"""
    SELECT semestre, groupe_laboratoire, 100 * part_de_marche AS part
    FROM gold_parts_de_marche WHERE groupe_laboratoire IN ({", ".join(f"'{l}'" for l in LABOS)})
""").pivot(index="semestre", columns="groupe_laboratoire", values="part")[LABOS]

fig, ax = plt.subplots(figsize=(11, 5.5))
x = range(len(parts))
for labo in LABOS:
    ax.plot(x, parts[labo], color=COULEURS[labo])
etiquettes_fin_de_courbe(ax, parts.reset_index(drop=True), len(parts) - 1, ecart_min=1.6,
                         valeur_en_texte=lambda v: f"{v:.1f} %")
ax.set_xticks(list(x), parts.index, rotation=0, fontsize=8.5)
ax.set_xlim(-0.3, len(parts) + 1.8)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f} %"))
ax.set_title("Part de marché par laboratoire et par semestre", pad=22)
sous_titre(ax, "En % des ventes d'antidiabétiques (ATC A10) ; les autres laboratoires (génériqueurs…) ne sont pas tracés")
plt.show()

# COMMAND ----------

evolution = (parts.iloc[[0, -1]].T.rename(columns=lambda s: f"part_{s}")
             .assign(evolution_points=lambda d: d.iloc[:, 1] - d.iloc[:, 0])
             .sort_values("evolution_points"))

fig, ax = plt.subplots(figsize=(9, 4))
ax.barh([NOMS[l] for l in evolution.index], evolution["evolution_points"],
        color=[HAUSSE if v > 0 else BAISSE for v in evolution["evolution_points"]], height=0.6)
for i, v in enumerate(evolution["evolution_points"]):
    ax.annotate(f"{v:+.1f} pts", xy=(v, i), xytext=(6 if v > 0 else -6, 0), textcoords="offset points",
                ha="left" if v > 0 else "right", va="center", color=ENCRE_2, fontsize=9)
ax.axvline(0, color="#c3c2b7", linewidth=1)
ax.grid(axis="y", visible=False); ax.grid(axis="x", visible=True)
ax.set_xlim(evolution["evolution_points"].min() - 5, evolution["evolution_points"].max() + 5)
ax.set_title(f"Évolution de la part de marché, {parts.index[0]} -> {parts.index[-1]}", pad=22)
sous_titre(ax, "En points de part de marché")
plt.show()

# COMMAND ----------

# Produits qui expliquent les mouvements : plus fortes hausses et baisses de ventes 2021 -> 2025
produits = sql("""
    SELECT produit, groupe_laboratoire, classe_therapeutique,
           sum(CASE WHEN annee = 2021 THEN ventes_eur ELSE 0 END) / 1e6 AS ventes_2021_meur,
           sum(CASE WHEN annee = 2025 THEN ventes_eur ELSE 0 END) / 1e6 AS ventes_2025_meur
    FROM gold_ventes_mensuelles GROUP BY ALL
""")
produits = (produits.groupby(["produit", "groupe_laboratoire", "classe_therapeutique"], as_index=False).sum()
            .assign(ecart_meur=lambda d: d.ventes_2025_meur - d.ventes_2021_meur))
display(pd.concat([produits.nlargest(8, "ecart_meur"), produits.nsmallest(6, "ecart_meur")]).round(1))

# COMMAND ----------

# MAGIC %md
# MAGIC **Lecture.** Le paysage concurrentiel a été redistribué en cinq ans :
# MAGIC - **AstraZeneca** (+19 points) et **Boehringer Ingelheim** (+9 points) sont les grands gagnants, portés
# MAGIC   chacun par une gliflozine (Forxiga : +379 M€, Jardiance : +146 M€) dont les indications se sont élargies
# MAGIC   au cœur et au rein.
# MAGIC - **MSD** s'effondre (-15 points) : Januvia et Janumet ont perdu leur exclusivité (génériques de la sitagliptine).
# MAGIC - **Novo Nordisk** reste leader mais perd 5 points : Ozempic progresse fortement (+253 M€), mais Victoza
# MAGIC   (-89 M€) et les insulines historiques reculent.
# MAGIC - **Sanofi** recule régulièrement (-6 points) avec le marché des insulines (Lantus : -41 M€).
# MAGIC - **Lilly** progresse jusqu'en 2023 (Trulicity), puis revient à son niveau de 2021 (≈ 20 %).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Qui domine chaque classe thérapeutique ?

# COMMAND ----------

dernier_semestre = spark.sql("SELECT max(semestre) AS s FROM gold_parts_de_marche_classe").first().s
classes_labos = sql(f"""
    SELECT classe_therapeutique, groupe_laboratoire, ventes_eur / 1e6 AS ventes_meur,
           100 * part_dans_la_classe AS part_dans_la_classe_pct, 100 * poids_de_la_classe AS poids_de_la_classe_pct
    FROM gold_parts_de_marche_classe WHERE semestre = '{dernier_semestre}'
""")
leaders = (classes_labos.sort_values("ventes_meur", ascending=False)
           .groupby("classe_therapeutique").head(2)
           .sort_values(["poids_de_la_classe_pct", "ventes_meur"], ascending=False))
print(f"Deux premiers laboratoires de chaque classe, {dernier_semestre} :")
display(leaders.round(1))

# COMMAND ----------

# MAGIC %md
# MAGIC **Lecture.** Chaque classe innovante est un **duopole** : GLP-1 = Novo Nordisk (Ozempic) face à Lilly
# MAGIC (Trulicity) ; SGLT2 = AstraZeneca (Forxiga) face à Boehringer Ingelheim (Jardiance) ; insulines = Novo
# MAGIC Nordisk, Sanofi et Lilly. Les classes anciennes (metformine, gliptines, sulfamides) sont dominées par les
# MAGIC génériqueurs et ne sont plus un enjeu de promotion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Investissements auprès des médecins

# COMMAND ----------

invest_annuel = sql(f"""
    SELECT left(semestre, 4) AS annee, groupe_laboratoire,
           sum(invest_total_eur) AS invest_total_eur, sum(invest_diabete_eur) AS invest_diabete_eur
    FROM gold_investissements GROUP BY ALL
""")
diabete = invest_annuel.pivot(index="annee", columns="groupe_laboratoire", values="invest_diabete_eur")[LABOS]

fig, ax = plt.subplots(figsize=(11, 5.5))
for labo in LABOS:
    ax.plot(diabete.index, diabete[labo], color=COULEURS[labo], marker="o", markersize=5)
etiquettes_fin_de_courbe(ax, diabete.reset_index(drop=True), len(diabete) - 1, ecart_min=70e3,
                         valeur_en_texte=lambda v: f"{v / 1e3:,.0f} k€".replace(",", " "))
ax.set_xlim(-0.3, len(diabete) + 0.9)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v / 1e6:.1f} M€"))
ax.set_title("Investissement diabète par laboratoire et par an", pad=22)
sous_titre(ax, "Avantages + rémunérations liés au diabète (événement diabète, ou endocrinologue si non précisé) - 2021 : Covid")
plt.show()

total = invest_annuel.groupby("groupe_laboratoire")[["invest_total_eur", "invest_diabete_eur"]].sum()
total["part_diabete_pct"] = 100 * total["invest_diabete_eur"] / total["invest_total_eur"]
display((total / [1e6, 1e6, 1]).round(2).sort_values("invest_diabete_eur", ascending=False).reset_index())

# COMMAND ----------

# Comment ? Répartition de l'investissement diabète par type d'action (en %)
def repartition(dimension, filtre="cible_diabete"):
    df = sql(f"""
        SELECT groupe_laboratoire, {dimension} AS modalite, sum(montant_verse_eur) AS montant
        FROM gold_investissements_detail WHERE {filtre} GROUP BY ALL
    """).pivot(index="groupe_laboratoire", columns="modalite", values="montant").fillna(0)
    return (100 * df.div(df.sum(axis=1), axis=0)).round(1).loc[LABOS]

print("Investissement diabète par type d'action (%)")
display(repartition("categorie_action").reset_index())
print("Investissement diabète par spécialité du médecin (%)")
display(repartition("specialite").reset_index())
print("Investissement total par thème de l'événement (%) - toutes aires thérapeutiques")
display(repartition("theme_evenement", "true").reset_index())

# COMMAND ----------

# Où ? Investissement diabète par région (région d'exercice du médecin bénéficiaire)
regions = sql("""
    SELECT region, sum(montant_verse_eur) AS invest_diabete_eur
    FROM gold_investissements_detail WHERE cible_diabete AND region <> 'Hors France / inconnue' GROUP BY region
""")
display(regions.assign(part_pct=lambda d: (100 * d.invest_diabete_eur / d.invest_diabete_eur.sum()).round(1))
        .sort_values("invest_diabete_eur", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Lecture.**
# MAGIC - **Lilly** est de loin le premier investisseur sur le diabète (4,9 M€ sur 2021-2025), devant **Sanofi**
# MAGIC   (3,0 M€) et **Novo Nordisk** (2,6 M€). Son effort augmente chaque année, quand celui de Novo Nordisk
# MAGIC   baisse depuis 2022.
# MAGIC - Le diabète ne représente qu'une **petite part** de l'investissement total des laboratoires généralistes
# MAGIC   (6 % chez AstraZeneca, moins de 2 % chez Servier et MSD, qui investissent dans d'autres aires
# MAGIC   thérapeutiques), alors que **Novo Nordisk** (40 %) et **Lilly** (30 %) sont les plus engagés sur le diabète.
# MAGIC - L'effort passe surtout par des **rémunérations** (orateurs, experts, consultants) et des invitations à
# MAGIC   des **congrès** ; les repas, très nombreux, pèsent entre 7 et 27 % des montants selon le laboratoire.
# MAGIC - Il cible d'abord les **endocrinologues**, mais Lilly (20 %) et Novo Nordisk (14 %) touchent aussi les
# MAGIC   **généralistes**, premiers prescripteurs du diabète de type 2, et Boehringer Ingelheim vise les
# MAGIC   **cardiologues** (31 %) et **néphrologues** (14 %), en cohérence avec le positionnement cardio-rénal de Jardiance.
# MAGIC - Par région (lieu d'exercice du médecin), l'**Île-de-France** arrive en tête (18 %), puis Occitanie,
# MAGIC   Provence-Alpes-Côte d'Azur et Auvergne-Rhône-Alpes (≈ 10 % chacune) ; l'**Outre-mer** pèse 6 %,
# MAGIC   ce qui est cohérent avec une prévalence du diabète plus élevée dans ces territoires.
# MAGIC
# MAGIC ⚠️ Chez AstraZeneca, Boehringer Ingelheim et MSD, la plupart des avantages ne citent pas de convention :
# MAGIC leur investissement diabète repose surtout sur la spécialité du médecin (d'où 93 % d'endocrinologues chez
# MAGIC AstraZeneca) et est probablement sous-estimé.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ciblage : couverture des endocrinologues et intensité promotionnelle

# COMMAND ----------

impact = sql("SELECT * FROM gold_impact ORDER BY semestre")
couverture = impact.pivot(index="semestre", columns="groupe_laboratoire", values="couverture_endocrinologues")[LABOS] * 100

fig, ax = plt.subplots(figsize=(11, 5.5))
x = range(len(couverture))
for labo in LABOS:
    ax.plot(x, couverture[labo], color=COULEURS[labo])
etiquettes_fin_de_courbe(ax, couverture.reset_index(drop=True), len(couverture) - 1, ecart_min=1.8,
                         valeur_en_texte=lambda v: f"{v:.1f} %")
ax.set_xticks(list(x), couverture.index, fontsize=8.5)
ax.set_xlim(-0.3, len(couverture) + 1.8)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f} %"))
ax.set_title("Couverture des endocrinologues par semestre", pad=22)
sous_titre(ax, "Part des endocrinologues-diabétologues en activité touchés au moins une fois dans le semestre")
plt.show()

# COMMAND ----------

dernier = impact[impact.semestre == impact.semestre.max()].set_index("groupe_laboratoire").loc[LABOS]

# Intensité sur l'année complète (deux semestres), plus stable qu'un semestre isolé
ANNEE_INTENSITE = impact.semestre.max()[:4]
annee = impact[impact.semestre.str.startswith(ANNEE_INTENSITE)].groupby("groupe_laboratoire")
intensite = (1000 * annee.invest_diabete_eur.sum() / annee.ventes_eur.sum()).loc[LABOS].sort_values()

fig, ax = plt.subplots(figsize=(9, 4))
ax.barh([NOMS[l] for l in intensite.index], intensite, color=[COULEURS[l] for l in intensite.index], height=0.6)
for i, v in enumerate(intensite):
    ax.annotate(f"{v:.2f} €", xy=(v, i), xytext=(6, 0), textcoords="offset points", va="center",
                color=ENCRE_2, fontsize=9)
ax.grid(axis="y", visible=False); ax.grid(axis="x", visible=True)
ax.set_title(f"Intensité promotionnelle, année {ANNEE_INTENSITE}", pad=22)
sous_titre(ax, "Euros investis (diabète) pour 1 000 € de ventes")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Lecture.** **Lilly** touche plus d'un tiers des endocrinologues en activité chaque semestre (la meilleure
# MAGIC couverture depuis le 2e semestre 2021) et investit environ 3 € pour 1 000 € de ventes. **Sanofi** a l'intensité la plus
# MAGIC forte (≈ 4 €) : il investit beaucoup pour défendre des ventes d'insulines en recul. À l'opposé, **Novo
# MAGIC Nordisk** et **AstraZeneca** dépensent environ 0,60 € pour 1 000 € de ventes : leurs produits phares
# MAGIC progressent sans effort promotionnel proportionné (forte demande pour Ozempic, nouvelles indications de Forxiga).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. L'investissement est-il suivi d'une hausse des ventes ?
# MAGIC
# MAGIC Pour chaque laboratoire et chaque semestre, on compare l'**évolution sur un an de l'investissement
# MAGIC diabète** à l'**évolution sur un an des ventes du semestre suivant**. On exclut :
# MAGIC - 2021 (Covid), qui sert de base aux évolutions de 2022 ;
# MAGIC - MSD et Servier, dont l'investissement diabète est quasi nul : la moindre variation y donne des
# MAGIC   évolutions de plusieurs milliers de %, sans signification.

# COMMAND ----------

SEUIL_INVEST = 50_000    # investissement diabète moyen minimal par semestre (€)
labos_investisseurs = (impact.groupby("groupe_laboratoire").invest_diabete_eur.mean()
                       .loc[lambda s: s >= SEUIL_INVEST].index)
print("Laboratoires retenus :", ", ".join(NOMS[l] for l in LABOS if l in labos_investisseurs))

points = impact[(impact.semestre >= "2022-S2")
                & impact.groupe_laboratoire.isin(labos_investisseurs)
                & impact.evolution_invest_diabete_annuelle.notna()
                & impact.evolution_ventes_semestre_suivant.notna()].copy()
points["d_invest"] = 100 * points.evolution_invest_diabete_annuelle
points["d_ventes_suivant"] = 100 * points.evolution_ventes_semestre_suivant

pearson = points.d_invest.corr(points.d_ventes_suivant)
spearman = points.d_invest.corr(points.d_ventes_suivant, method="spearman")

fig, ax = plt.subplots(figsize=(9, 5.5))
ax.scatter(points.d_invest, points.d_ventes_suivant, s=40, color="#2a78d6", edgecolor="#fcfcfb", linewidth=1.5)
ax.axhline(0, color="#c3c2b7", linewidth=1); ax.axvline(0, color="#c3c2b7", linewidth=1)
ax.grid(axis="x", visible=True)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.0f} %"))
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.0f} %"))
ax.set_xlabel("Évolution sur un an de l'investissement diabète (semestre t)")
ax.set_ylabel("Évolution sur un an des ventes (semestre t + 1)")
ax.set_title("Investissement et ventes du semestre suivant", pad=22)
sous_titre(ax, f"{len(points)} observations laboratoire × semestre - corrélation de Pearson {pearson:.2f}, "
               f"de Spearman {spearman:.2f}")
plt.show()

par_labo = pd.DataFrame([
    {"laboratoire": NOMS[labo], "nb_semestres": len(d), "correlation": round(d.d_invest.corr(d.d_ventes_suivant), 2)}
    for labo, d in points.groupby("groupe_laboratoire")])
display(par_labo)

# COMMAND ----------

# MAGIC %md
# MAGIC **Lecture.** La corrélation de Pearson (≈ 0,3) est tirée par deux points extrêmes : Boehringer Ingelheim
# MAGIC en 2023, quand les ventes de Jardiance triplaient en même temps que l'investissement augmentait. La
# MAGIC corrélation de **Spearman**, qui compare les rangs et résiste à ces valeurs extrêmes, est **quasi nulle** :
# MAGIC une hausse de l'investissement n'est pas suivie, en général, d'une hausse des ventes au semestre suivant.
# MAGIC Les corrélations par laboratoire vont de -0,9 à +0,4 et reposent sur **6 points seulement** : elles ne
# MAGIC sont pas interprétables statistiquement.
# MAGIC
# MAGIC Ce résultat est cohérent avec le marché étudié : les ventes sont d'abord portées par des facteurs
# MAGIC **structurels** (nouvelles indications des gliflozines, génériques des gliptines, pénuries d'Ozempic,
# MAGIC déclin des insulines), bien plus que par l'effort promotionnel d'un semestre. Transparence Santé ne
# MAGIC couvre en outre qu'une partie de la promotion (pas les visites médicales ni la publicité).
# MAGIC **Conclusion : l'analyse est descriptive ; elle ne permet pas de mesurer un retour sur investissement.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Synthèse : positionnement concurrentiel

# COMMAND ----------

synthese = pd.DataFrame({
    "part_2025_pct": parts.iloc[-1],
    "evolution_part_pts": evolution["evolution_points"],
    f"invest_diabete_{ANNEE_INTENSITE}_keur": annee.invest_diabete_eur.sum() / 1e3,
    f"intensite_{ANNEE_INTENSITE}_eur_pour_1000": intensite,
    "couverture_endocrino_dernier_semestre_pct": 100 * dernier["couverture_endocrinologues"],
}).loc[LABOS]
synthese.columns = ["part_pct", "evolution_part_pts", "invest_diabete_keur", "intensite", "couverture_pct"]

fig, ax = plt.subplots(figsize=(9.5, 6))
ax.scatter(synthese.intensite, synthese.evolution_part_pts, s=synthese.part_pct * 25 + 30,
           color=[COULEURS[l] for l in synthese.index], edgecolor="#fcfcfb", linewidth=2, alpha=0.9)
for labo, ligne in synthese.iterrows():
    ax.annotate(f"{NOMS[labo]} ({ligne.part_pct:.0f} %)", xy=(ligne.intensite, ligne.evolution_part_pts),
                xytext=(10, 6), textcoords="offset points", color=ENCRE, fontsize=9.5)
ax.axhline(0, color="#c3c2b7", linewidth=1)
ax.grid(axis="x", visible=True)
ax.set_xlabel(f"Intensité promotionnelle {ANNEE_INTENSITE} (€ investis pour 1 000 € de ventes)")
ax.set_ylabel(f"Évolution de la part de marché {parts.index[0]} -> {parts.index[-1]} (points)")
ax.set_title("Positionnement des laboratoires", pad=22)
sous_titre(ax, "Taille des bulles : part de marché actuelle (indiquée entre parenthèses)")
plt.show()

display(synthese.rename(index=NOMS, columns={
    "part_pct": f"part_{parts.index[-1]}_pct", "invest_diabete_keur": f"invest_diabete_{ANNEE_INTENSITE}_keur",
    "intensite": f"eur_pour_1000_eur_ventes_{ANNEE_INTENSITE}", "couverture_pct": f"couverture_endocrino_{parts.index[-1]}_pct",
}).round(2).reset_index(names="laboratoire"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Lecture - quatre profils se dégagent :**
# MAGIC
# MAGIC | Profil | Laboratoires | Caractéristiques |
# MAGIC |---|---|---|
# MAGIC | **Conquérants portés par l'innovation** | AstraZeneca, Boehringer Ingelheim | Forte hausse de part de marché grâce aux gliflozines, avec une promotion diabète modeste : la croissance vient des nouvelles indications cardio-rénales. |
# MAGIC | **Leader installé** | Novo Nordisk | Première part de marché, faible intensité promotionnelle : Ozempic se vend sans effort proportionné, mais le portefeuille ancien (Victoza, insulines) s'érode. |
# MAGIC | **Challenger offensif** | Lilly | Le plus fort investissement (1,3 M€ en 2025) et la meilleure couverture des endocrinologues, pour une part de marché revenue à son niveau de 2021 : défense de Trulicity face à Ozempic. |
# MAGIC | **Défenseur d'un portefeuille en déclin** | Sanofi | Intensité promotionnelle la plus élevée pour des ventes d'insulines en recul. |
# MAGIC | **Sortis du marché de la promotion diabète** | MSD, Servier | Produits génériqués, quasi aucun investissement diabète. |
# MAGIC
# MAGIC **Limites** : marché de ville uniquement (pas l'hôpital) ; gliflozines en partie prescrites hors diabète ;
# MAGIC Transparence Santé ne déclare ni le produit promu ni la visite médicale ; ciblage diabète estimé à partir
# MAGIC de l'événement et de la spécialité ; 10 semestres seulement pour relier investissement et ventes.
