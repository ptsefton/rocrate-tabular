from pathlib import Path
from rocrate_tabular.tabulator import ROCrateTabulator


def test_minimal(crates, tmp_path):
    dbfile = Path(tmp_path) / "sqlite.db"
    tb = ROCrateTabulator()
    tb.crate_to_db(crates["minimal"], dbfile)


def test_wide(crates, tmp_path):
    dbfile = Path(tmp_path) / "sqlite.db"
    tb = ROCrateTabulator()
    tb.crate_to_db(crates["wide"], dbfile)


def test_config(crates, tmp_path):
    cwd = Path(tmp_path)
    dbfile = cwd / "sqlite.db"
    conffile = cwd / "config.json"
    tb = ROCrateTabulator()
    tb.crate_to_db(crates["minimal"], dbfile)
    tb.infer_config()
    tb.write_config(conffile)
    # smoke test to make sure another tabulator can read the config
    tb2 = ROCrateTabulator()
    tb2.crate_to_db(crates["minimal"], dbfile)
    tb2.load_config(conffile)
