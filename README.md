# Coronavirus Traffic Congestion Impact in Latin America with Waze Data

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=EL-BID_Covid-19-Traffic-Impact-Dashboard&metric=alert_status)](https://sonarcloud.io/dashboard?id=EL-BID_Covid-19-Traffic-Impact-Dashboard)
![analytics image (flat)](https://raw.githubusercontent.com/vitr/google-analytics-beacon/master/static/badge-flat.gif)
![analytics](https://www.google-analytics.com/collect?v=1&cid=555&t=pageview&ec=repo&ea=open&dp=/IDB-IDB-Invest-Coronavirus-Impact-Dashboard/readme&dt=&tid=UA-4677001-16)



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


:warning: All the data in the daily and weekly series has been replaced by a new version of the series, because duplicated data was found for specific regions and dates in the data published in 2020. This problem has now been fixed, but **there could be differences between the data in this version of the Dashboard and prior version**. Data in the latest update of the Dashboard is available only until July 11th, 2021 as internat changes are being made to the process of downloading and storing data. Updated information will become available in Octobre 2021. 

:warning: In addition, due to unexpected difficulties with downloading and re-processing the data, right now we are not providing TCI changes for the countries of Brazil and Mexico (but we do provide their cities). This will be solved in the next few weeks. 


## Use the data

The data was also though to be used by the broad community of researchers, journalists and developers. If you have any ideas or doubts about using the data, do not hesitate to submit an [issue](https://github.com/EL-BID/IDB-IDB-Invest-Coronavirus-Impacto-Dashboard/issues/new).

But first, make sure you understand the data:
- [Data Dictionary](docs/Data%20Dictionary.md)
- The [Methodological Note](https://iadb-comms.org/COVID19-Impact-Dashboard-Methodological-Note)  will also continuously track and update methodological changes (when necessary) and changes/additions to data sources. The version of the Methodological Note and its date of creation are shown at the top of the document.

The latest version of the data is easily available through the methods below.

:warning: By mistake, traffic congestion information for Fortaleza (Brazil) was being reported as Natal (Brazil). This has been solved, and now "Natal" reflects the traffic congestion for that metropolitan area, and Fortaleza data is reported under "Fortaleza".

### Download Manually

- [daily](https://drive.google.com/file/d/1w7agEtKX54c7pGW5Ua1Q8d0cyZZvFZAM/view?usp=sharing)
- [weekly](https://drive.google.com/file/d/1DGpJAjpGRjKCaIXH6Zg0P7Iq6yPzM8zd/view?usp=sharing)
- [metadata](https://docs.google.com/spreadsheets/d/1PAS6uPwJ3nX-oh-6__-OPO8PXUA87yg_/edit?usp=sharing&ouid=101752774292135053408&rtpof=true&sd=true)

Access the [sheets](http://tiny.cc/idb-traffic-sheets)


### Python

```
import pandas as pd

daily = pd.read_csv('[daily]')
weekly = pd.read_csv('[weekly]')
metadata = pd.read_csv('[metadata]')
```

### R

```
library(readr)

daily<-read_csv('[daily]')
weekly<-read_csv('[weekly]')
metadata<-read_csv('[metadata])
```

### Stata

```
import delimited using "[daily]", clear
import delimited using "[metadata]", clear
import delimited using "[metadata]", clear
```

## Waze for Cities Partners

You can find materials prepared for Waze for Cities Partnership Partners [here](https://github.com/EL-BID/IDB-IDB-Invest-Coronavirus-Impact-Dashboard/tree/master/waze_for_cities).

- [TCI Percentage Variation Tutorial](http://tiny.cc/idb-tci-bigquery-tutorial)

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

- IDB: João Carabetta, Daniel Martinez, Edgar Salgado, Beatrice Zimmermann, Sonia Mendizabal 

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
