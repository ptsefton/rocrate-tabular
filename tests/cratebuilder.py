# crate builder

import json

LICENSE_ID = ("https://creativecommons.org/licenses/by-nc-sa/3.0/au/",)
LICENSE_DESCRIPTION = """
This work is licensed under the Creative Commons 
Attribution-NonCommercial-ShareAlike 3.0 Australia License.
To view a copy of this license, visit
http://creativecommons.org/licenses/by-nc-sa/3.0/au/ or send a letter to
Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
"""
LICENSE_IDENTIFIER = "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"
LICENSE_NAME = """
Attribution-NonCommercial-ShareAlike 3.0 Australia (CC BY-NC-SA 3.0 AU)"
"""


class JsonCrateException(Exception):
    pass


class JsonCrate:
    def __init__(self):
        self.context = "https://w3id.org/ro/crate/1.1/context"
        self.graph = []

    def add(self, t, i, props):
        json_props = dict(props)
        json_props["@id"] = i
        json_props["@type"] = t
        self.graph.append(json_props)

    def get(self, i):
        es = [e for e in self.graph if e["@id"] == i]
        if es:
            return es[0]
        else:
            return None

    def root(self):
        metadata = self.get("ro-crate-metadata.json")
        if metadata is None:
            raise JsonCrateException("no ro-crate-metadata.json entity")
        root = self.get(metadata["about"]["@id"])
        if root is None:
            raise JsonCrateException("malformed or missing root entity")
        return root

    def json(self):
        return json.dumps({"@context": self.context, "@graph": self.graph}, indent=2)


def minimal_crate(name="Minimal crate", description="Minimal crate"):
    """Create ROCrate json with the minimal structure"""
    crate = JsonCrate()
    license_id = "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"
    crate.add(
        "Dataset",
        "./",
        {
            "name": name,
            "description": description,
            "license": {"@id": license_id},
        },
    )
    crate.add(
        "CreativeWork",
        license_id,
        {
            "name": LICENSE_NAME,
            "description": "LICENSE_DESCRIPTION",
            "identifier": "LICENSE_IDENTIFIER",
        },
    )
    crate.add(
        "CreativeWork",
        "ro-crate-metadata.json",
        {
            "about": {"@id": "./"},
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
        },
    )
    return crate


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


wcrate = make_wide_dataset("wide_test")

print(wcrate.json())
