# Coronavirus Traffic Congestion Impact in Latin America with Waze Data

Follow  the impact of Coronavirus outbreak in Latin America in **real time**.

![landing_dash](https://github.com/EL-BID/Covid-19-Traffic-Impact-Dashboard/blob/master/imgs/dashboard_landing.png?raw=true)
Go to dashboard: [ENGLISH](https://www.iadb.org/en/topics-effectiveness-improving-lives/coronavirus-impact-dashboard)
[ESPAÑOL](https://www.iadb.org/es/topics-effectiveness-improving-lives/coronavirus-impact-dashboard)

## Use the data

The data was also though to be used by the broad community of researchers, journalists and developers. The latest version is easily available 

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

Make sure you understand the data:
- [Data Dictionary](https://github.com/EL-BID/Covid-19-Traffic-Impact-Dashboard/blob/master/docs/Data%20Dictionary.md)
- [Methodology](https://iadb-comms.org/COVID19-Impact-Dashboard-Methodological-Note) This Methodological Note will also continuously track and update methodological changes (when necessary) and changes/additions to data sources. The version of the Methodological Note and its date of creation are shown at the top of the document.

## Ask for a specific region

The way that the pipeline was implemented allow us to query any region in Latin
America. To ask for an specific region(s) of interest please submit an issue.

[Click here to request regions](https://github.com/EL-BID/Covid-19-Traffic-Impact-Dashboard/issues/new?assignees=JoaoCarabetta&labels=enhancement&template=region-request.md&title=%5BRegion+Request%5D+%3Cadd+short+description%3E)

## Don't forget to cite us :)


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

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

### Acknowledgments

* This README was adapted from [*A template to make good README.md*](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* The structure of this repository was adapted from [*Fast Project Templates*](https://github.com/JoaoCarabetta/project-templates)

### About
This repository reflects the code being used in the most current version of the dashboard
