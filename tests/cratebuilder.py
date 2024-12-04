# crate builder

from rocrate_tabular.tinycrate import minimal_crate


def make_wide_dataset(dir):
    """Make a crate with 1000+ hasParts from the root dataset"""
    crate = minimal_crate(name="Wide", description="Wide crate")
    re = crate.root()
    re["hasPart"] = []
    for i in range(2000):
        fid = f"{i:04d}.txt"
        re["hasPart"].append({"@id": fid})
        crate.add("File", fid, {"name": fid, "encodingFormat": "text/utf-8"})
    return crate
