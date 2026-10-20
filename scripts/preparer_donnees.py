"""
Ingestion des données sources : téléchargement, préparation et envoi vers Databricks.

Pourquoi ce script ? Databricks Free Edition bloque l'accès à Internet depuis les notebooks.
Le téléchargement est donc fait ailleurs (GitHub Actions, dans le cloud), puis les fichiers
sont déposés dans le Volume Databricks via l'API Files (workspace > sfe_diabete > Volumes > brut).

Aucune donnée n'est modifiée, sauf le fichier RPPS qui est réduit :
  - médecins uniquement (code profession 10) ;
  - colonnes utiles seulement : pas de nom, prénom, téléphone, e-mail ni adresse (RGPD).

Utilisation :
  - GitHub Actions (.github/workflows/ingestion.yml) : variables d'environnement DATABRICKS_HOST
    et DATABRICKS_TOKEN définies -> envoi automatique dans le Volume ;
  - en local, sans ces variables : création de data/donnees_sfe_diabete.zip à importer à la main.
"""
import gzip
import json
import os
import shutil
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
BRUT = RACINE / "data" / "brut"
A_IMPORTER = RACINE / "data" / "a_importer"
ARCHIVE = RACINE / "data" / "donnees_sfe_diabete.zip"

ANNEES = [2021, 2022, 2023, 2024, 2025]

CATALOGUE, SCHEMA, VOLUME = "workspace", "sfe_diabete", "brut"

# ameli.fr refuse (erreur 403) les requêtes sans identification de navigateur
USER_AGENT = "Mozilla/5.0 (compatible; projet-sfe-diabete; +https://github.com/HouamelOmar/projet-sfe-diabete)"

LABOS_TRANSPARENCE = [
    "NOVO NORDISK",
    "SANOFI WINTHROP INDUSTRIE", "SANOFI AVENTIS FRANCE",
    "LILLY FRANCE", "LILLY FRANCE SAS",
    "ASTRAZENECA",
    "BOEHRINGER INGELHEIM FRANCE",
    "MSD FRANCE",
    "LES LABORATOIRES SERVIER",
]

COLONNES_RPPS = [
    "Type d'identifiant PP", "Identifiant PP", "Identification nationale PP",
    "Code profession", "Libellé profession",
    "Code catégorie professionnelle", "Libellé catégorie professionnelle",
    "Code type savoir-faire", "Libellé type savoir-faire",
    "Code savoir-faire", "Libellé savoir-faire",
    "Code mode exercice", "Libellé mode exercice",
    "Code postal (coord. structure)", "Code commune (coord. structure)",
    "Libellé commune (coord. structure)",
    "Code Département (structure)", "Libellé Département (structure)",
    "Code secteur d'activité", "Libellé secteur d'activité",
    "Code rôle", "Libellé rôle",
]


def telecharger(url, destination, params=None):
    """Télécharge un fichier, avec reprise automatique si la connexion coupe."""
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    for tentative in range(10):
        deja = destination.stat().st_size if destination.exists() else 0
        entetes = {"User-Agent": USER_AGENT}
        if deja:
            entetes["Range"] = f"bytes={deja}-"
        requete = urllib.request.Request(url, headers=entetes)
        try:
            with urllib.request.urlopen(requete, timeout=120) as reponse:
                if deja and reponse.status != 206:   # le serveur ne gère pas la reprise
                    deja = 0
                with open(destination, "ab" if deja else "wb") as f:
                    shutil.copyfileobj(reponse, f, length=8 * 1024 * 1024)
            print(f"  {destination.name} : {destination.stat().st_size / 1e6:.1f} Mo")
            return
        except urllib.error.HTTPError as erreur:
            if erreur.code == 416:                   # fichier déjà complet
                print(f"  {destination.name} : déjà complet")
                return
            if erreur.code in (403, 404):            # refus ou fichier absent : inutile de réessayer
                print(f"  {url} -> erreur {erreur.code}")
                raise
            print(f"  erreur {erreur}, nouvelle tentative...")
        except Exception as erreur:
            print(f"  erreur {erreur}, nouvelle tentative...")
        time.sleep(5)
    raise RuntimeError(f"Échec du téléchargement : {url}")


def medicam():
    print("Medic'AM")
    for annee in ANNEES:
        for periode in ["01-a-06", "07-a-12"]:
            fichier = BRUT / "medicam" / f"{annee}-{periode}.zip"
            if fichier.exists():
                continue
            # le lien data.gouv.fr du 2e semestre 2025 est erroné : on construit l'adresse
            for suffixe in ["", "_0"]:
                try:
                    telecharger("https://www.assurance-maladie.ameli.fr/sites/default/files/"
                                f"{annee}-{periode}_medic-am-par-type-de-prescripteur_serie-mensuelle"
                                f"{suffixe}.zip", fichier)
                    break
                except urllib.error.HTTPError as erreur:
                    fichier.unlink(missing_ok=True)
                    if erreur.code != 404:               # seul un 404 justifie d'essayer l'autre adresse
                        raise
            else:
                raise RuntimeError(f"Medic'AM {annee} {periode} introuvable")


def bdpm():
    print("Base publique des médicaments")
    for nom in ["CIS_bdpm.txt", "CIS_CIP_bdpm.txt", "CIS_GENER_bdpm.txt"]:
        fichier = BRUT / "bdpm" / nom
        if not fichier.exists():
            telecharger(f"https://base-donnees-publique.medicaments.gouv.fr/download/file/{nom}", fichier)


def transparence():
    print("Transparence Santé")
    fichier = BRUT / "transparence" / "declarations_labos_diabete_2021_2025.csv"
    if fichier.exists():
        return
    labos = ", ".join(f'"{l}"' for l in LABOS_TRANSPARENCE)
    filtre = (f"raison_sociale in ({labos}) "
              f"and date >= date'{ANNEES[0]}-01-01' and date < date'{ANNEES[-1] + 1}-01-01' "
              f'and beneficiaire_profession_code = "10"')
    colonnes = ("id,raison_sociale,numero_siren,lien_interet,motif_lien_interet,autre_motif,"
                "information_evenement,convention_liee,montant,date,date_debut,date_fin,"
                "beneficiaire_identifiant,beneficiaire_type_code,beneficiaire_profession_code,"
                "code_postal,pays_code,statut")   # pas de nom ni d'adresse (RGPD)
    telecharger("https://www.transparence.sante.gouv.fr/api/explore/v2.1/catalog/datasets/declarations/exports/csv",
                fichier, params={"select": colonnes, "where": filtre, "delimiter": ";"})


def rpps():
    print("Annuaire Santé RPPS")
    fichier = BRUT / "rpps" / "personne_activite.txt"
    with urllib.request.urlopen(urllib.request.Request(
            "https://www.data.gouv.fr/api/1/datasets/annuaire-sante-extractions-des-donnees-en-libre-acces-"
            "des-professionnels-intervenant-dans-le-systeme-de-sante-rpps/",
            headers={"User-Agent": USER_AGENT}), timeout=60) as r:
        ressource = next(x for x in json.load(r)["resources"] if "personne-activite" in x["title"])
    if not fichier.exists() or fichier.stat().st_size < (ressource.get("filesize") or float("inf")):
        telecharger(ressource["url"], fichier)
    if ressource.get("filesize") and fichier.stat().st_size != ressource["filesize"]:
        raise RuntimeError("Fichier RPPS incomplet : relancer le script (le téléchargement reprendra)")


def reduire_rpps():
    """Garde uniquement les médecins et les colonnes utiles (sans données personnelles)."""
    source = BRUT / "rpps" / "personne_activite.txt"
    cible = A_IMPORTER / "rpps" / "rpps_medecins.txt"
    cible.parent.mkdir(parents=True, exist_ok=True)
    lues = gardees = 0
    with open(source, encoding="utf-8", newline="") as entree, \
         open(cible, "w", encoding="utf-8", newline="") as sortie:
        entete = entree.readline().rstrip("\r\n").split("|")
        index = [entete.index(c) for c in COLONNES_RPPS]
        i_profession = entete.index("Code profession")
        sortie.write("|".join(COLONNES_RPPS) + "\n")
        for ligne in entree:
            champs = ligne.rstrip("\r\n").split("|")
            lues += 1
            if len(champs) == len(entete) and champs[i_profession] == "10":
                sortie.write("|".join(champs[i] for i in index) + "\n")
                gardees += 1
    print(f"  RPPS : {lues:,} lignes lues, {gardees:,} lignes de médecins gardées "
          f"({cible.stat().st_size / 1e6:.1f} Mo)".replace(",", " "))


def preparer_import():
    """Copie dans data/a_importer/ les fichiers à glisser dans le Volume Databricks."""
    print("Préparation du dossier a_importer")
    for dossier, motif in [("medicam", "*.zip"), ("bdpm", "*.txt"), ("transparence", "*.csv")]:
        (A_IMPORTER / dossier).mkdir(parents=True, exist_ok=True)
        for fichier in sorted((BRUT / dossier).glob(motif)):
            shutil.copy2(fichier, A_IMPORTER / dossier / fichier.name)
    reduire_rpps()
    # Compression gzip : envoi ~8 fois plus léger, et Spark lit directement les fichiers .gz
    for fichier in [A_IMPORTER / "transparence" / "declarations_labos_diabete_2021_2025.csv",
                    A_IMPORTER / "rpps" / "rpps_medecins.txt"]:
        with open(fichier, "rb") as entree, gzip.open(f"{fichier}.gz", "wb") as sortie:
            shutil.copyfileobj(entree, sortie)
        fichier.unlink()
    for dossier in sorted(A_IMPORTER.iterdir()):
        fichiers = list(dossier.iterdir())
        taille = sum(f.stat().st_size for f in fichiers) / 1e6
        print(f"  {dossier.name:13s} {len(fichiers):3d} fichier(s)  {taille:7.1f} Mo")


def creer_archive():
    # Un seul fichier à importer dans le Volume : le notebook 02 le décompresse dans les bons dossiers.
    # ZIP_STORED (sans recompression) car les fichiers sont déjà compressés.
    with zipfile.ZipFile(ARCHIVE, "w", zipfile.ZIP_STORED) as z:
        for fichier in sorted(A_IMPORTER.rglob("*")):
            if fichier.is_file():
                z.write(fichier, fichier.relative_to(A_IMPORTER).as_posix())
    print(f"\nFichier unique à importer : {ARCHIVE} ({ARCHIVE.stat().st_size / 1e6:.1f} Mo)")


def api_databricks(methode, chemin, corps=None, donnees=None, taille=None):
    """Appel à l'API REST Databricks avec le jeton personnel (secret GitHub)."""
    entetes = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    if corps is not None:
        donnees = json.dumps(corps).encode()
        entetes["Content-Type"] = "application/json"
    elif donnees is not None:
        entetes["Content-Type"] = "application/octet-stream"
        entetes["Content-Length"] = str(taille)
    hote = os.environ["DATABRICKS_HOST"].rstrip("/")
    requete = urllib.request.Request(f"{hote}{chemin}", data=donnees, method=methode, headers=entetes)
    with urllib.request.urlopen(requete, timeout=600) as reponse:
        return reponse.status


def creer_si_absent(chemin, corps):
    try:
        api_databricks("POST", chemin, corps=corps)
        print(f"  créé : {corps['name']}")
    except urllib.error.HTTPError as erreur:
        if erreur.code not in (400, 409) or "ALREADY_EXISTS" not in erreur.read().decode():
            raise
        print(f"  existe déjà : {corps['name']}")


def envoyer_vers_databricks():
    """Dépose les fichiers préparés dans le Volume Unity Catalog via l'API Files."""
    print("Envoi vers Databricks")
    creer_si_absent("/api/2.1/unity-catalog/schemas",
                    {"catalog_name": CATALOGUE, "name": SCHEMA})
    creer_si_absent("/api/2.1/unity-catalog/volumes",
                    {"catalog_name": CATALOGUE, "schema_name": SCHEMA, "name": VOLUME,
                     "volume_type": "MANAGED"})
    racine_volume = f"/Volumes/{CATALOGUE}/{SCHEMA}/{VOLUME}"
    for fichier in sorted(A_IMPORTER.rglob("*")):
        if not fichier.is_file():
            continue
        cible = f"{racine_volume}/{fichier.relative_to(A_IMPORTER).as_posix()}"
        api_databricks("PUT", f"/api/2.0/fs/directories{cible.rsplit('/', 1)[0]}")
        taille = fichier.stat().st_size
        with open(fichier, "rb") as f:
            api_databricks("PUT", f"/api/2.0/fs/files{urllib.parse.quote(cible)}?overwrite=true",
                           donnees=f, taille=taille)
        print(f"  {cible} ({taille / 1e6:.1f} Mo)")


if __name__ == "__main__":
    medicam()
    bdpm()
    transparence()
    rpps()
    preparer_import()
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        envoyer_vers_databricks()
        print("\nTerminé. Les fichiers sont dans le Volume : lancer le notebook 02_bronze_tables.")
    else:
        creer_archive()
        print(f"\nTerminé. Importer {ARCHIVE.name} à la racine du Volume workspace > sfe_diabete > brut")
