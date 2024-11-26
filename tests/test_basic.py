from pathlib import Path
from rocrate_tabular.rocrate_tabular import tosqlite


def test_basic(crates, tmp_path):
    dbfile = Path(tmp_path) / "sqlite.db"
    tosqlite(crates["minimal"], dbfile)
    

