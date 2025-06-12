from pathlib import Path
from rocrate_tabular.tabulator import ROCrateTabulator
import json


def read_config(cffile):
    with open(cffile, "r") as cfh:
        return json.load(cfh)


def write_config(cf, cffile):
    with open(cffile, "w") as cfh:
        json.dump(cf, cfh)


def test_export(crates, tmp_path):
    cwd = Path(tmp_path)
    dbfile = Path(tmp_path) / "lf.db"
    conffile = cwd / "config.json"
    tb = ROCrateTabulator()
    tb.crate_to_db(crates["languageFamily"], dbfile)
    tb.infer_config()
    tb.write_config(conffile)
    tb.close()

    # load the config and move the potential tables to tables
    cf = read_config(conffile)
    cf["tables"]["RepositoryObject"] = cf["potential_tables"]["RepositoryObject"]
    write_config(cf, conffile)

    tb = ROCrateTabulator()
    tb.load_config(conffile)
    tb.crate_to_db(crates["languageFamily"], dbfile)

    tb.entity_table("RepositoryObject", None)

    cf = read_config(conffile)
    cf["export_queries"] = {"lf.csv": "SELECT * FROM RepositoryObject"}
    write_config(cf, conffile)

    tb2 = ROCrateTabulator()
    tb2.load_config(conffile)
    tb2.crate_to_db(crates["languageFamily"], dbfile)

    csvout = cwd / "csv"

    tb2.export_csv(csvout)
