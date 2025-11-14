## pyrefsearch

<!---Badges-->
[![Python 3.10](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

## Fonctions:
Recherche de publications dans la base de données OpenAlex et de brevets
   dans la base de données INPADOC pour une liste d'auteur.e.s sur une
   gamme d'années donnée.

## Pré-requis:
- Une clé API est requise pour faire des recherches de brevets dans la base de données
  INPADOC (*International Patent Documentation*) gérée par l'EPO (*European
  patent office*) et accessible via *espacenet*. Il faut d'abord ouvrir un compte en ligne pour accéder aux
  [*Open Patent Services (OPS)*](https://developers.epo.org/user/register) de l'EPO. Une fois
  le compte activé, il faut définir les variables environnementales
  *PATENT_CLIENT_EPO_API_KEY* et *PATENT_CLIENT_EPO_SECRET* sur le poste de travail tel qu'expliqué dans
  [*How to get an EPO OPS API key*](https://patent-client.readthedocs.io/en/stable/getting_started.html).

## Utilisation:
- Appel du script: *pyrefsearch\pyrefsearch.py data\pyrefsearch.toml*
- Les paramètres d'exécution sont lus dans le fichier *pyrefsearch.toml*
- Les données d'auteur.e.s sont lues dans un fichier Excel d'entrée (voir *pyrefsearch.toml*)
- Les résultats de la recherche sont écrits dans un fichier Excel de sortie
- Tous les fichiers spécifiés sont lus/écrits dans le même répertoire que le fichier .toml

<p></p>

## *pyrefsearch.toml* : fichier des paramètres d'exécution pour une recherche sur une période donnée (voir le fichier donné en exemple) :
- *in_excel_file* : Fichier Excel d'entrée
- *date_start*, *date_end* : Première/dernière dates de la période de recherche
- *publication_types_openalex* : List des binômes de types de publications et mots-clefs correspondants ("subtype")
  pour la recherche de publications dans OpenAlex
- *local_affiliations* : Liste des institutions considérées comme des affiliations "locales"
- *espacenet_patent_search* : activer la recherche de brevets ("espacenet_patent_search = true")
- *espacenet_patent_search_results_file* = nom de fichier de résultats de recherche dans espacenet précédante (si ce paramètre
n'est pas spécifié, une nouvelle recherche en ligne est effectuée, ce qui est assez long)

<div style="page-break-after: always;"></div>

## *pyrefsearch_last_month.toml* : fichier des paramètres d'exécution pour une recherche au cours du dernier mois (voir le fichier donné en exemple) :
Pour forcer une recherche dans le mois précédant, en plus des paramètres ci-haut (*pyrefsearch.toml*) où
*date_start*, *date_end* sont ignorés, les paramètres suivants sont requis:
- *previous_month_publications_search* = true
- *previous_month_publications_search_confirmation_emails* : list des adresses qui recevront un courriel avec le fichier Excel de résultats

## Fichier Excel d'entrée "*in_excel_file*" (voir fichier *pyrefsearch.toml*), feuillet *Membres* :
Noms et identifiants OpenAlex des auteur.e.s spécifiés dans les colonnes
suivantes :

- *Nom* : Nom de famille de l’auteur.e
- *Prénom* : Prénom de l’auteur.e
- *OpenAlex* : Adresse web du profil OpenAlex de l’auteur.e pour la recherche de publications 
(laisser la cellule vide si aucun identifiant n'est disponible)

## Fichier Excel de sortie "*\<in_excel_file mantissa\>\_publications\_\<date_start\>-\<date_end\>.xlsx*" :
  - Feuillet *Résultats* : résumé des résultats de la recherche
  - Feuillets *Articles*, *Conférences*, *Lettres*, *Livres*, *Chap. de livres*,
  *Pré-impression*, *HAL*, *Rapports*, *Brevets INPADOC (en instance)*,
  *Brevets INPADOC (délivrés)* : résultats de la
  recherche par type de document (omis si aucun résultat)
  - Feuillet *Auteurs - Profils*, colonne *Erreurs*: erreurs le cas échéant dans
  les profils d'auteur.e.s et/ou disparités entre les informations dans le fichier Excel
  d'entrée et la base de données OpenAlex
  - Feuillet *Auteurs - Homonymes*, colonne *Affl/ID*:
  repérage des noms d’auteur.e.s
  correspondant à plusieurs identifiants OpenAlex, afin de s’assurer que les bons
  identifiants sont fournis dans le fichier Excel d’entrée pour la recherche de
  publications & brevets:
    - *Affl.* : l'affiliation est comprise dans la liste *local_affiliations*
      spécifiée dans *pyrefsearch.toml*
    - *ID* : l'identifiant OpenAlex est identique à celui dans le fichier Excel d'entrée

## Installation:
- Projet *pyrefsearch* disponible sur [github](https://github.com/pgcharetteUdeS/pyRefSearchUdeS)
- *pip install -r requirements.txt*
