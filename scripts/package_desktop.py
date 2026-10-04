"""Assemble the local portable app, source, and registry license notices.

Does not install, publish, replace configuration, or start any process.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", type=Path, default=ROOT / "target/slint-ui/debug/tkfs.exe")
    parser.add_argument("--output", type=Path, default=ROOT / "target/desktop-portable")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    shutil.copy2(args.exe, output / "tkfs.exe")
    for name in ("README.md", "THIRD-PARTY-NOTICES.md", "LICENSE-GPL-3.0.txt", "LICENSE-SLINT-ROYALTY-FREE-2.0.md"):
        shutil.copy2(ROOT / name, output / name)
    result = subprocess.run(["cargo","metadata","--offline","--locked","--filter-platform","x86_64-pc-windows-msvc","--format-version","1"],cwd=ROOT,capture_output=True,text=True,check=True)
    metadata = json.loads(result.stdout)
    selected = {node["id"] for node in metadata["resolve"]["nodes"]}
    inventory = []
    notice_root = output / "licenses"
    notice_root.mkdir(exist_ok=True)
    for package in sorted(metadata["packages"],key=lambda p:(p["name"],p["version"])):
        if package["id"] not in selected or package["source"] is None:
            continue
        root = Path(package["manifest_path"]).parent
        dest = notice_root / f'{package["name"]}-{package["version"]}'
        files = []
        candidates = [p for p in root.iterdir() if p.is_file() and p.name.lower().startswith(("license","copying","notice","copyright"))]
        for folder in (root / "LICENSES",root / "licenses"):
            if folder.is_dir():
                candidates.extend(p for p in folder.rglob("*") if p.is_file())
        if package.get("license_file"):
            candidates.append(root / package["license_file"])
        for source in dict.fromkeys(candidates):
            if not source.is_file():
                continue
            dest.mkdir(exist_ok=True)
            filename = source.relative_to(root).as_posix().replace("/","-")
            shutil.copy2(source,dest / filename)
            files.append((dest / filename).relative_to(output).as_posix())
        inventory.append({"name":package["name"],"version":package["version"],"license":package["license"],"authors":package["authors"],"repository":package["repository"],"notices":files})
    (output / "DEPENDENCY-LICENSES.json").write_text(json.dumps(inventory,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    with zipfile.ZipFile(output / "telekinesis-source.zip","w",zipfile.ZIP_DEFLATED) as archive:
        for folder in ("src","ui","native","scripts","tests"):
            for file in (ROOT / folder).rglob("*"):
                if file.is_file() and "__pycache__" not in file.parts:
                    archive.write(file,file.relative_to(ROOT).as_posix())
        for filename in ("Cargo.toml","Cargo.lock","build.rs","README.md","ORCHESTRATOR-PLAN.md","THIRD-PARTY-NOTICES.md","LICENSE-GPL-3.0.txt","LICENSE-SLINT-ROYALTY-FREE-2.0.md"):
            archive.write(ROOT / filename,filename)
    print(json.dumps({"executable":str(output / "tkfs.exe"),"dependency_inventory_count":len(inventory),"packages_without_bundled_license_files":[f'{p["name"]} {p["version"]}' for p in inventory if not p["notices"]]},indent=2))

if __name__ == "__main__":
    main()
