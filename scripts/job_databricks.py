"""
Orchestration Databricks : mise à jour du code puis lancement du job de traitement.

Appelé par GitHub Actions après l'ingestion (scripts/preparer_donnees.py) :
  1. met à jour le dossier Git Databricks du projet sur la dernière version de main (équivalent du « Pull ») ;
  2. crée ou met à jour le job « SFE diabète - pipeline » (02 -> 03 -> 04 -> 05 et 06) ;
  3. lance le job et affiche le lien vers son exécution.

Les tâches tournent sur le calcul serverless (aucun cluster à gérer, seul mode disponible sur Free Edition).
Variables d'environnement : DATABRICKS_HOST et DATABRICKS_TOKEN (secrets GitHub).
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

DEPOT = "github.com/houamelomar/projet-sfe-diabete"
NOM_JOB = "SFE diabète - pipeline"

# (tâche, notebook, tâches dont elle dépend)
TACHES = [
    ("bronze", "02_bronze_tables", []),
    ("silver", "03_silver_nettoyage", ["bronze"]),
    ("gold", "04_gold_indicateurs", ["silver"]),
    ("analyse", "05_analyse", ["gold"]),
    ("tableau_de_bord", "06_tableau_de_bord", ["gold"]),
]


def api(methode, chemin, corps=None, parametres=None):
    hote = os.environ["DATABRICKS_HOST"].rstrip("/")
    url = f"{hote}{chemin}" + (f"?{urllib.parse.urlencode(parametres)}" if parametres else "")
    requete = urllib.request.Request(
        url, method=methode, data=json.dumps(corps).encode() if corps is not None else None,
        headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(requete, timeout=120) as reponse:
            contenu = reponse.read()
            return json.loads(contenu) if contenu else {}
    except urllib.error.HTTPError as erreur:
        raise RuntimeError(f"{methode} {chemin} -> {erreur.code} : {erreur.read().decode()[:500]}") from None


def toutes_les_pages(chemin, cle, parametres=None):
    elements, jeton = [], None
    while True:
        reponse = api("GET", chemin, parametres={**(parametres or {}), **({"page_token": jeton} if jeton else {})})
        elements += reponse.get(cle, [])
        jeton = reponse.get("next_page_token")
        if not jeton:
            return elements


def mettre_a_jour_dossier_git(email):
    """Trouve le dossier Git Databricks relié au dépôt GitHub et le place sur la dernière version de main."""
    normaliser = lambda url: url.lower().removeprefix("https://").removesuffix(".git").rstrip("/")
    # Sans path_prefix, l'API ne renvoie que les dossiers Git de /Workspace/Repos (ancien emplacement) :
    # on cherche aussi dans le dossier personnel, où Databricks les crée aujourd'hui
    trouves = []
    for prefixe in [f"/Workspace/Users/{email}", None]:
        trouves += toutes_les_pages("/api/2.0/repos", "repos", {"path_prefix": prefixe} if prefixe else None)
    dossiers = [d for d in trouves if normaliser(d.get("url", "")) == DEPOT]
    if not dossiers:
        print("Dossiers Git trouvés :", [(d.get("path"), d.get("url")) for d in trouves] or "aucun")
        raise RuntimeError(
            "Aucun dossier Git Databricks relié au dépôt. Dans Databricks : Workspace > Create > Git folder, "
            f"adresse https://{DEPOT}, puis relancer.")
    dossier = dossiers[0]
    api("PATCH", f"/api/2.0/repos/{dossier['id']}", {"branch": "main"})
    print(f"Dossier Git mis à jour : {dossier['path']} (branche main)")
    return dossier["path"]


def definition_job(dossier, email):
    return {
        "name": NOM_JOB,
        "description": "Bronze -> Silver -> Gold -> analyse et tableau de bord (projet SFE diabète).",
        "max_concurrent_runs": 1,
        "timeout_seconds": 3 * 3600,
        "email_notifications": {"on_failure": [email]},
        "tasks": [
            {
                "task_key": tache,
                "depends_on": [{"task_key": d} for d in dependances],
                "notebook_task": {"notebook_path": f"{dossier}/notebooks/{notebook}", "source": "WORKSPACE"},
                "timeout_seconds": 3600,
            }
            for tache, notebook, dependances in TACHES
        ],
    }


def creer_ou_mettre_a_jour_job(definition):
    existants = [j for j in toutes_les_pages("/api/2.1/jobs/list", "jobs", {"name": NOM_JOB})
                 if j["settings"]["name"] == NOM_JOB]
    if existants:
        job_id = existants[0]["job_id"]
        api("POST", "/api/2.1/jobs/reset", {"job_id": job_id, "new_settings": definition})
        print(f"Job mis à jour : {NOM_JOB} (id {job_id})")
    else:
        job_id = api("POST", "/api/2.1/jobs/create", definition)["job_id"]
        print(f"Job créé : {NOM_JOB} (id {job_id})")
    return job_id


if __name__ == "__main__":
    email = api("GET", "/api/2.0/preview/scim/v2/Me")["userName"]
    dossier = mettre_a_jour_dossier_git(email)
    job_id = creer_ou_mettre_a_jour_job(definition_job(dossier, email))
    run_id = api("POST", "/api/2.1/jobs/run-now", {"job_id": job_id})["run_id"]
    lien = f"{os.environ['DATABRICKS_HOST'].rstrip('/')}/jobs/{job_id}/runs/{run_id}"
    print(f"Job lancé : {lien}")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as resume:
            resume.write(f"### Job Databricks lancé\n[{NOM_JOB}]({lien})\n")
