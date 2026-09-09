"""Hash every package inside the built .ispac files.

The manifest this writes is what the catalog parity check compares the deployed
project streams against: a project that builds clean but was never redeployed is
indistinguishable from a deployed one by project and package counts alone.
"""

import argparse
import hashlib
import json
import os
import zipfile


def package_hashes(ispac_path):
    hashes = {}
    with zipfile.ZipFile(ispac_path) as archive:
        for entry in archive.namelist():
            if not entry.lower().endswith(".dtsx"):
                continue
            with archive.open(entry) as handle:
                hashes[os.path.basename(entry)] = hashlib.sha256(handle.read()).hexdigest().upper()
    return hashes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", help="directory holding the built .ispac files")
    parser.add_argument("--output", required=True, help="manifest path to write")
    args = parser.parse_args()

    manifest = {}
    for name in sorted(os.listdir(args.directory)):
        if not name.lower().endswith(".ispac"):
            continue
        manifest[os.path.splitext(name)[0]] = package_hashes(os.path.join(args.directory, name))

    with open(args.output, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")

    total = sum(len(packages) for packages in manifest.values())
    print("projects %d" % len(manifest))
    print("packages %d" % total)


if __name__ == "__main__":
    main()
