# Coronavirus Traffic Congestion Impact in Latin America with Waze Data

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=EL-BID_Covid-19-Traffic-Impact-Dashboard&metric=alert_status)](https://sonarcloud.io/dashboard?id=EL-BID_Covid-19-Traffic-Impact-Dashboard)

Follow  the impact of Coronavirus outbreak in Latin America in **real time**. 

![landing_dash](https://github.com/EL-BID/Covid-19-Traffic-Impact-Dashboard/blob/master/imgs/dashboard_landing.png?raw=true)
Go to dashboard: [ENGLISH](https://www.iadb.org/en/topics-effectiveness-improving-lives/coronavirus-impact-dashboard)
[ESPAÑOL](https://www.iadb.org/es/topics-effectiveness-improving-lives/coronavirus-impact-dashboard)

The Coronavirus Impact Dashboard has been created by the IDB and IDB Invest to track in real
time the impact of the coronavirus (also known as COVID-19) on the countries of Latin America
and the Caribbean. The dashboard aims to track a range of variables of interest in order to provide
policymakers, epidemiologists, and the general public in the region with measures of the impact
that “social distancing” restrictions and recommendations due to the coronavirus outbreak are
having on the population and on economic activity.

## Use the data

The data was also though to be used by the broad community of researchers, journalists and developers. If you have any ideas or doubts about using the data, do not hesitate to submit an [issue](https://github.com/EL-BID/IDB-IDB-Invest-Coronavirus-Impacto-Dashboard/issues/new).

But first, make sure you understand the data:
- [Data Dictionary](docs/Data%20Dictionary.md)
- The [Methodological Note](https://iadb-comms.org/COVID19-Impact-Dashboard-Methodological-Note)  will also continuously track and update methodological changes (when necessary) and changes/additions to data sources. The version of the Methodological Note and its date of creation are shown at the top of the document.

The latest version of the data is easily available through the methods below.

### Mannualy download

[Download](https://docs.google.com/spreadsheets/d/16SIYidLScgFZOeqpHmAo_u_rFmuxxpCCWeRAXSDOT3I/export?format=csv&id)

### Python

```
import pandas as pd
url = 'https://docs.google.com/spreadsheets/d/16SIYidLScgFZOeqpHmAo_u_rFmuxxpCCWeRAXSDOT3I/export?format=csv&id'
df = pd.read_csv(url)
```

### R

```
library(readr)

df<-read.csv('https://docs.google.com/spreadsheets/d/16SIYidLScgFZOeqpHmAo_u_rFmuxxpCCWeRAXSDOT3I/export?format=csv&id')
```
Obs: Not sure if it works. Submit a PR if you find a way to do it.

### Stata

```
import delimited using "https://docs.google.com/spreadsheets/d/16SIYidLScgFZOeqpHmAo_u_rFmuxxpCCWeRAXSDOT3I/export?format=csv&id", clear
```

## Ask for a specific region

The way that the pipeline was implemented allow us to query any region in Latin
America. To ask for an specific region(s) of interest please submit an issue.

[Click here to request regions](https://github.com/EL-BID/IDB-IDB-Invest-Coronavirus-Impacto-Dashboard/issues/new?assignees=JoaoCarabetta&labels=enhancement&template=region-request.md&title=%5BRegion+Request%5D+%3Cadd+short+description%3E)

## Don't forget to cite us :)

To cite the IDB and IDB Invest Coronavirus Impact Dashboard, please use the following reference:

> Inter-American Development Bank and IDB Invest. "IDB And IDB Invest Coronavirus Impact Dashboard". 2020. Inter-American Development Bank.www.iadb.org/coronavirus-impact-dashboard

To cite this Methodological Note, please use the following reference: 

> Inter-American Development Bank and IDB Invest. IDB And IDB Invest Coronavirus Impact Dashboard Methodological Note. Washington, DC: Inter-American Development Bank, 2020.https://iadb-comms.org/coronavirus-impact-dashboard-methodological-note.

## Team 

Development Effectiveness Division Chiefs 

- IDB: Carola Alvarez 

- IDB Invest: Alessandro Maffioli 

Technical Team Leaders 

- IDB: Oscar Mitnik 

- IDB Invest: Patricia Yañez-Pagans 

Technical Team 

- IDB: João Carabetta, Daniel Martinez, Edgar Salgado, Beatrice Zimmermann 

- IDB Invest: Mattia Chiapello, Luciano Sanguino

Communications Team 

- IDB: Lina Botero, Andrés Gómez-Peña 

- IDB Invest: Norah Sullivan 

IT Team 

- IDB: eBFactory  

- IDB Invest: Maiquel Sampaio de Melo 

### License

This work is licensed under a Creative Commons IGO 3.0 - see the [LICENSE.md](LICENSE.md) file for details.

### Acknowledgments

* This README was adapted from [*A template to make good README.md*](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* The structure of this repository was adapted from [*Fast Project Templates*](https://github.com/JoaoCarabetta/project-templates)

### About
This repository reflects the code being used in the most current version of the dashboard at the *Traffic Congestion* tab.
