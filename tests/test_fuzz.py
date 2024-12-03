from pathlib import Path
from cratebuilder import minimal_crate
from random import choice, randint
import string

from rocrate_tabular.rocrate_tabular import ROCrateTabulator


def random_word():
    n = randint(1, 10) + randint(0, 5)
    return "".join([choice(string.printable) for _ in range(n)])


def random_text(m):
    n = randint(1, m)
    return " ".join([random_word() for _ in range(n)])


def random_property():
    return random_word(), random_text(5)


def test_random(tmp_path):
    jcrate = minimal_crate()
    for i in range(1000):
        props = {"name": random_text(4), "description": random_text(11)}
        for j in range(30):
            prop, val = random_property()
            props[prop] = val
        jcrate.add("Dataset", f"#ds{i:05d}", props)
    crate_dir = Path(tmp_path) / "crate"
    crate_dir.mkdir()
    jcrate.write_json(crate_dir)
    db_file = Path(tmp_path) / "sqlite.db"
    tb = ROCrateTabulator()
    tb.crate_to_db(crate_dir, db_file)
    # loop through the crate's graph and try to find every entity and check
    # the properties are all there
    for entity in jcrate.graph:
        db_props = list(tb.fetch_entity(entity["@id"]))
        assert db_props
        # build a dict from the props // this should get promoted to the lib
        db_entity = {"@id": entity["@id"]}
        for db_prop in db_props:
            if db_prop["target_id"]:
                db_entity[db_prop["property_label"]] = {"@id": db_prop["target_id"]}
            else:
                db_entity[db_prop["property_label"]] = db_prop["value"]
        assert db_entity == entity


def test_expanded_properties(tmp_path):
    assert True
