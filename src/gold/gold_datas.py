"""Raccourci "tout regenerer" : appelle, dans l'ordre du DAG de
dependances, les 11 scripts de `src/gold/build/` qui produisent
individuellement chaque fichier `datas/gold/postgres_*.csv`,
`datas/gold/influxdb_*.csv` et `datas/gold/mqtt_iot_plc_send.jsonl`.

Chaque `build()` appele ici se verifie lui-meme (ne recalcule pas si son
csv de sortie existe deja) et resout ses propres dependances amont s'il en
a besoin - cf. `openspec/changes/split-gold-datas-per-dataset/design.md`.
L'ordre ci-dessous suit le DAG (niveau 0 puis niveau 1) mais n'est qu'une
commodite : chaque script fonctionne aussi appele seul, dans n'importe quel
ordre.
"""

from gold.build import (
    build_age_machine,
    build_machine,
    build_machine_secteur_historique,
    build_maintenance_alerte_influxdb,
    build_maintenance_alerte_postgres,
    build_mqtt_jsonl,
    build_nominale_values,
    build_production_status,
    build_regime_cadence,
    build_type_machine,
    build_type_metal,
)


def main() -> None:
    # Niveau 0 : bronze/silver uniquement
    build_type_machine.build()
    build_type_metal.build()
    build_regime_cadence.build()
    build_production_status.build()
    build_age_machine.build()
    build_machine_secteur_historique.build()

    build_maintenance_alerte_postgres.build_maintenance()
    build_maintenance_alerte_postgres.build_alerte()

    # Niveau 1 : dependent d'un csv gold voisin deja produit ci-dessus
    build_machine.build()
    build_nominale_values.build()
    #build_mqtt_jsonl.build()
    build_maintenance_alerte_influxdb.build_maintenance()
    build_maintenance_alerte_influxdb.build_alerte()


if __name__ == "__main__":
    main()
