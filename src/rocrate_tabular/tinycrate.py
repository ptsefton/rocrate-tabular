# a new, tiny python RO-Crate library with emphasis on manipulating
# the json-ld metadata

from pathlib import Path
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


class TinyCrateException(Exception):
    pass


class TinyCrate:
    def __init__(self, jsonld=None):
        if jsonld is not None:
            self.context = jsonld["@context"]
            self.graph = jsonld["@graph"]
        else:
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
            raise TinyCrateException("no ro-crate-metadata.json entity")
        root = self.get(metadata["about"]["@id"])
        if root is None:
            raise TinyCrateException("malformed or missing root entity")
        return root

    def json(self):
        return json.dumps({"@context": self.context, "@graph": self.graph}, indent=2)

    def write_json(self, crate_dir):
        with open(Path(crate_dir) / "ro-crate-metadata.json", "w") as jfh:
            json.dump({"@context": self.context, "@graph": self.graph}, jfh, indent=2)


def minimal_crate(name="Minimal crate", description="Minimal crate"):
    """Create ROCrate json with the minimal structure"""
    crate = TinyCrate()
    license_id = "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"
    crate.add(
        "Dataset",
        "./",
        {
            "name": name,
            "description": description,
            "license": {"@id": license_id},
            "datePublished": "2024",
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
