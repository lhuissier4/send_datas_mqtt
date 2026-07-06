# Tests HTTP InfluxDB 3

`influxdb.http` contient des requetes pretes a l'emploi (healthcheck, ecriture,
requete SQL, test sans token) pour `mspr2-influxdb-live` et
`mspr2-influxdb-staging`. A utiliser avec l'extension VSCode **REST Client**
(`humao.rest-client`) : ouvrir le fichier, cliquer sur "Send Request"
au-dessus de chaque bloc.

Les valeurs par defaut (host/port/token) correspondent a `.env.example` ;
adaptez-les si vous avez change ces variables dans votre `.env`.

## Se connecter avec InfluxDB 3 Explorer (UI)

Si la connexion echoue avec l'URL `http://localhost:<port>` dans l'UI, c'est
parce que l'Explorer tourne dans son propre conteneur Docker : `localhost`
depuis ce conteneur pointe vers lui-meme, pas vers InfluxDB. Explorer et les
deux instances InfluxDB de ce projet partagent le meme reseau Docker
(`send_datas_mqtt_default`), donc il faut utiliser le nom du service et le
port interne (`8181`, pas le port mappe cote hote) :

| Base             | URL a saisir dans l'Explorer          | Token                              |
|------------------|----------------------------------------|-------------------------------------|
| `sensor_live`    | `http://mspr2-influxdb-live:8181`      | `apiv3_mspr2-live-dev-token`        |
| `sensor_staging` | `http://mspr2-influxdb-staging:8181`   | `apiv3_mspr2-staging-dev-token`     |

Le port mappe cote hote (8183/8182 par defaut) ne sert que pour les appels
depuis l'exterieur de Docker (curl, ce fichier `.http`, les scripts Python).
