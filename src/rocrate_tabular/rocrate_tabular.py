from rocrate.rocrate import ROCrate
from argparse import ArgumentParser
from pathlib import Path
from sqlite_utils import Database
import csv
import os
import json

PROPERTIES = {
    "row_id": str,
    "source_id": str,
    "source_name": str,
    "property_label": str,
    "target_id": str,
    "value": str,
}


def get_as_list(v):
    """Ensures that a value is a list"""
    if v is None:
        return []
    if type(v) is list:
        return v
    return [v]


def get_as_id(v):
    """If v is an ID, return it or else return None"""
    if type(v) is dict:
        mid = v.get("@id", None)
        if mid is not None:
            return mid
    return None


def entity_properties(crate, e):
    """Returns a generator which yields all of this entity's rows"""
    eid = e.properties().get("@id", None)
    if eid is None:
        return
    ename = e.properties().get("name", "")
    for key, value in e.properties().items():
        if key != "@id":
            for v in get_as_list(value):
                maybe_id = get_as_id(v)
                if maybe_id is not None:
                    yield relation_row(crate, eid, ename, key, maybe_id)
                else:
                    yield property_row(eid, ename, key, v)


def relation_row(crate, eid, ename, prop, tid):
    target = crate.dereference(tid)
    if target:
        target_name = target.properties().get("name", "")
        return {
            "source_id": eid,
            "source_name": ename,
            "property_label": prop,
            "target_id": tid,
            "value": target_name,
        }
    else:
        return property_row(eid, ename, prop, target)


def property_row(eid, ename, prop, value):
    return {
        "source_id": eid,
        "source_name": ename,
        "property_label": prop,
        "value": value,
    }


def export_csv(main_config, db):
    queries = main_config["export_queries"]
    for csv_file, query in queries.items():
        result = list(db.query(query))
        # Convert result into a CSV file using csv writer
        with open(csv_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=result[0].keys(), quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()
            for row in result:
                for key, value in row.items():
                    if isinstance(value, str):
                        row[key] = value.replace("\n", "\\n").replace("\r", "\\r")
                writer.writerow(row)

        print(f"Exported data to {csv_file}")


def tosqlite(cratedir, dbfile):
    """Write a tabulated crate to an SQLite file"""
    db = Database(dbfile, recreate=True)
    properties = db["property"].create(PROPERTIES)
    crate = ROCrate(cratedir)
    seq = 0
    propList = []
    for e in crate.get_entities():
        for row in entity_properties(crate, e):
            row["row_id"] = seq
            seq += 1
            propList.append(row)
    properties.insert_all(propList)
    return db


def test(cratedir):
    """Test"""
    crate = ROCrate(cratedir)
    seq = 0
    for e in crate.get_entities():
        for row in entity_properties(crate, e):
            row["row_id"] = seq
            seq += 1
            print(row)


def find_csv(input_path):
    query = """
        SELECT source_id
        FROM property
        WHERE property_label = '@type' AND value = 'File' AND LOWER(source_id) LIKE '%.csv'
    """
    files = db.query(query)
    entity_ids = [row["source_id"] for row in files]

    print(entity_ids)
    for entity_id in entity_ids:
        entity_id = entity_id.replace("#", "")
        add_csv(db, os.path.join(input_path, entity_id), "csv_files")


def setup_config(name, db):
    config_file = f"{name}-config.json"

    if not os.path.exists(config_file):
        default_config = {"export_queries": {}, "tables": {}, "potential_tables": {}}
        query = """
            SELECT DISTINCT(p.value)
            FROM property p
            WHERE p.property_label = '@type'
        """
        types = db.query(query)

        for attype in [row["value"] for row in types]:
            default_config["potential_tables"][attype] = {
                "all_props": [],
                "ignore_props": [],
                "expand_props": [],
            }

        # Default configuration
        # default_config = {
        #     "export_queries": {},
        #     "tables": {
        #         "RepositoryObject": {"all_props": [],  # All properties found for all RepositoryObject entities
        #                              "ignore_props": [],  # Properties to ignore
        #                              # Default properties to expand
        #                              "expand_props": ["citation"]},
        #         "Person": {"all_props": [], "ignore_props": [], "expand_props": []}
        #     }
        #
        # }
        with open(config_file, "w") as f:
            json.dump(default_config, f, indent=4)
        print(f"Created default config file: {config_file}")

        main_config = default_config
    else:
        # Read configuration
        with open(config_file, "r") as f:
            main_config = json.load(f)

    return main_config


def save_config(main_config, name):
    config_file = f"{name}-config.json"
    # Save the updated configuration file
    with open(config_file, "w") as f:
        json.dump(main_config, f, indent=4)
    print(
        f"Updated config file: {config_file}, edit this file to change the flattening configuration or deleted it to start over"
    )

    return main_config


def flatten_entities(db, input_path, main_config, text):
    print("Building flat tables")

    for table in main_config["tables"]:
        # Step 1: Query to get list of @id for entities with @type = table
        print(f"Flattening table for entites of type: {table}")
        # We want to find where an @type is RepositoryObject
        query = f"""
            SELECT p.source_id
            FROM property p
            WHERE p.property_label = '@type' AND p.value = '{table}'
        """
        print(query)
        repository_objects = db.query(query)
        config = main_config["tables"][table]
        # Convert the result to a list of @id values
        entity_ids = [row["source_id"] for row in repository_objects]

        print(entity_ids)

        # Step 2: For each source_id, retrieve all its associated properties
        for entity_id in entity_ids:
            # Query to get all properties for the specific @id
            properties = db.query(
                """
                SELECT property_label, value, target_id
                FROM property
                WHERE source_id = ?
            """,
                [entity_id],
            )

            # Create a dictionary to hold the properties for this entity
            entity_data = {"entity_id": entity_id}

            # Step 3: Loop through properties and add them to entity_data
            props = []
            for prop in properties:
                property_name = prop["property_label"]
                property_value = prop["value"]
                property_target = prop["target_id"]
                props.append(property_name)

                if text is not None and property_name == text:
                    print("text", property_target, property_value)
                    # Check if the value is a valid file name

                    ### HACK: Work around for the fact that the RO-Crate libary does not import File entities it does not like
                    if not property_target:
                        p = json.loads(property_value)
                        property_target = p.get("@id")

                    text_file = os.path.join(input_path, property_target)
                    if os.path.isfile(text_file):
                        # Read the text from the file
                        with open(text_file, "r") as f:
                            text_contents = f.read()
                        # Add the text to the entity_data dictionary
                        entity_data[property_name] = text_contents

                    else:
                        print(f"File not found: {text_file}")

                # If the property is in the props_to_expand list, expand it
                if property_name in config["expand_props"] and property_target:
                    # Query to get the
                    sub_query = """
                        SELECT p.property_label, p.value, p.target_id
                        FROM property p
                        WHERE p.source_id = ? 
                    """
                    expanded_properties = db.query(sub_query, [property_target])

                    # Add each sub-property (e.g., author.name, author.age) to the entity_data dictionary
                    # Is this the indexableText property?

                    for expanded_prop in expanded_properties:
                        expanded_property_name = (
                            f"{property_name}_{expanded_prop['property_label']}"
                        )
                        props.append(expanded_property_name)
                        # Special case - if this is indexable text then we want to read t

                        if expanded_property_name not in config["ignore_props"]:
                            set_property_name(
                                entity_data,
                                expanded_property_name,
                                expanded_prop["value"],
                            )
                            if expanded_prop["target_id"]:
                                set_property_name(
                                    entity_data,
                                    f"{expanded_property_name}_id",
                                    expanded_prop["target_id"],
                                )
                else:
                    # If it's a normal property, just add it to the entity_data dictionary
                    if property_name not in config["ignore_props"]:
                        set_property_name(entity_data, property_name, property_value)
                        if property_target:
                            set_property_name(
                                entity_data, f"{property_name}_id", property_target
                            )

            config["all_props"] = list(set(config["all_props"] + props))
            # Step 4: Insert the flattened properties into the 'flat_entites' table
            (
                db[f"{table}"].insert(
                    entity_data, pk="entity_id", replace=True, alter=True
                ),
            )

    print("Flattened entities table created")

    return main_config


def set_property_name(entity_data, property_name, property_value):
    if property_name in entity_data:
        # Find the first available integer to append to property_name
        i = 1
        while f"{property_name}_{i}" in entity_data:
            i += 1
        property_name = f"{property_name}_{i}"
    entity_data[property_name] = property_value


def add_csv(db, csv_path, table_name):
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)  # Use DictReader to read each row as a dictionary
        rows = list(reader)
        if rows:
            # Insert rows into the table (the table will be created if it doesn't exist)
            db[table_name].insert_all(rows, pk="id", alter=True, ignore=True)
            # `pk="id"` assumes there's an 'id' column; if no primary key, you can remove it.


if __name__ == "__main__":
    ap = ArgumentParser("RO-Crate to tables")
    ap.add_argument(
        "crate",
        type=Path,
        help="RO-Crate directory",
    )
    ap.add_argument(
        "output",
        type=Path,
        help="Output file (.csv or .db)",
    )
    ap.add_argument(
        "-n", "--name", default="", type=str, help="Write the name of the config"
    )
    ap.add_argument(
        "-t",
        "--text",
        default=None,
        type=str,
        help="Property label that references a file containing text for an entity",
    )
    ap.add_argument(
        "--csv",
        action="store_true",
        help="Find CSV files and concatenate them into a table",
    )
    args = ap.parse_args()

    db = tosqlite(args.crate, args.output)
    main_config = setup_config(args.name, db)
    new_config = flatten_entities(db, args.crate, main_config, args.text)
    save_config(new_config, args.name)

    if args.csv:
        find_csv(args.crate)
    export_csv(main_config, db)
