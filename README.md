# air_quality_data

## CONTEXTE

TotalGreen, une entreprise française spécialisée dans les énergies renouvelables, lance GoodAir, un laboratoire de recherche axé sur la qualité de l'air et de l'eau en France. L'objectif principal de ce laboratoire est de surveiller et d'analyser la qualité de l'air et de l'eau, afin de fournir des recommandations à la population, d'étudier les impacts du changement climatique et d'établir des seuils d'alerte. En outre, il vise à mener des recherches scientifiques approfondies dans ce domaine tout en développant des plateformes de sensibilisation pour le grand public.

Le laboratoire est constitué d'une équipe multidisciplinaire d'une dizaine de chercheurs et d'analystes spécialisés dans les domaines du climat, de la biologie et de la météorologie. Pour mener à bien leurs recherches, ils ont besoin de données fiables, accessibles et pertinentes. Afin d'optimiser les coûts et le temps de collecte des données, le directeur du laboratoire souhaite s'appuyer sur des sources de données déjà existantes.

Dans ce contexte, GoodAir cherche à récupérer et à stocker un ensemble d'informations pertinentes pour ses chercheurs. Ces données doivent être accessibles via un outil de visualisation des données et être exportables pour des analyses plus poussées. Ainsi, le laboratoire sollicite une expertise externe pour auditer le projet et s'assurer de sa pertinence et de sa faisabilité.

## Architecture proposée

![architecture_mspr drawio](https://github.com/Oiver237/air_quality_data/assets/73575249/6513ebec-1606-4c34-ba25-831406d285ce)

## Requirements

- VS code
- Python 3.8 au moins
- AWS account
- Airflow
- Docker

## data sources

- Les données relatives à la qualité de l’air[1] : https://aqicn.org/json-api/doc/
- Les données météorologiques1 : https://openweathermap.org/api

