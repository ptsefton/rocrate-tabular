# from pathlib import Path

import json
from pathlib import Path
from fuzz import random_text, random_property

from rocrate_tabular.tinycrate import TinyCrate, minimal_crate

# https://pytest-httpserver.readthedocs.io/en/latest/ for testing fetch


def test_crate(tmp_path):
    """Build and save and reload a crate"""
    jcrate = minimal_crate()
    for i in range(1000):
        props = {"name": random_text(4), "description": random_text(11)}
        for j in range(30):
            prop, val = random_property()
            props[prop] = val
        jcrate.add("Dataset", f"#ds{i:05d}", props)
    jsonf = Path(tmp_path) / "ro-crate-metadata.json"
    jcrate.write_json(Path(tmp_path))
    with open(jsonf, "r") as jfh:
        jsonld = json.load(jfh)
        jcrate2 = TinyCrate(jsonld=jsonld)
        for i in range(1000):
            eid = f"#ds{i:05d}"
            ea = jcrate.get(eid)
            eb = jcrate2.get(eid)
            assert ea.props == eb.props


def test_load_file(crates):
    cratedir = crates["textfiles"]
    with open(Path(cratedir) / "ro-crate-metadata.json", "r") as jfh:
        jsonld = json.load(jfh)
        crate = TinyCrate(jsonld=jsonld, directory=cratedir)
        tfile = crate.get("textfile.txt")
        contents = tfile.fetch()
        with open(Path(cratedir) / "textfile.txt", "r") as tfh:
            contents2 = tfh.read()
            assert contents == contents2
