# Archive des fichiers Medic'AM (2021-2025)

Copie **non modifiée** des fichiers officiels publiés par l'Assurance Maladie (Cnam) :
*Medic'AM - médicaments remboursés par l'Assurance Maladie, par type de prescripteur, série mensuelle*.

- Source : https://www.data.gouv.fr/fr/datasets/medicam-medicaments-rembourses-par-lassurance-maladie-par-type-de-prescripteur-donnees-interregimes/
- Producteur : Caisse nationale de l'Assurance Maladie (Cnam)
- Licence : [Licence Ouverte / Open Licence](https://www.etalab.gouv.fr/licence-ouverte-open-licence) (réutilisation et redistribution autorisées avec mention de la source)

## Pourquoi cette archive ?

Le site `assurance-maladie.ameli.fr` refuse les connexions des serveurs situés hors de France
(erreur 403), dont ceux de GitHub Actions. Le pipeline (`scripts/preparer_donnees.py`) tente toujours
le téléchargement officiel en premier et n'utilise cette copie qu'en cas de refus.

Les semestres 2021 à 2025 sont définitifs : ces fichiers ne sont plus modifiés par la Cnam.

| Fichier | Période |
|---|---|
| `2021-01-a-06.zip` | janvier - juin 2021 |
| `2021-07-a-12.zip` | juillet - décembre 2021 |
| ... | ... |
| `2025-07-a-12.zip` | juillet - décembre 2025 |
