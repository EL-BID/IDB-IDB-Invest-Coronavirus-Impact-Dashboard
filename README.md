# Covid-19 Traffic Congestion Impact in Latin America with Waze Data

Follow  the impact of COVID-19 outbreak in Latin America in **real time**.

[IMAGE PLACEHOLDER WITH LINK]

## Use the data

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
Obs: I am not sure if it works. Submit a PR if you find a way to do it.

### Stata

```
import delimited using "https://docs.google.com/spreadsheets/d/16SIYidLScgFZOeqpHmAo_u_rFmuxxpCCWeRAXSDOT3I/export?format=csv&id", clear
```

## Ask for an specific region

The way that the pipeline was implemented allow us to query any region in Latin
America. To ask for an specific region(s) of interest please submit an issue.

[CLICK HERE TO REQUEST REGIONS](https://github.com/EL-BID/Covid-19-Traffic-Impact-Dashboard/issues/new?assignees=JoaoCarabetta&labels=enhancement&template=region-request.md&title=%5BRegion+Request%5D+%3Cadd+short+description%3E)

## Methodological Note



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


### License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

### Acknowledgments

* This README was adapted from [*A template to make good README.md*](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* The structure of this repository was adapted from [*Fast Project Templates*](https://github.com/JoaoCarabetta/project-templates)
