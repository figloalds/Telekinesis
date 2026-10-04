# Telekinesis desktop notices

Slint 1.17.0, copyright © SixtyFPS GmbH and the Slint developers,
https://slint.dev, is used under the Slint Royalty-free Desktop, Mobile, and Web
Applications License version 2.0. The app's top-level About button opens an About
screen containing the official AboutSlint widget, satisfying attribution route
2(a). The selected dependency license is bundled in LICENSE-SLINT-ROYALTY-FREE-2.0.md.
No external license account, payment, or commercial agreement was created.

Slint's selected route does not impose GPL on the application's own code.
Independently, this repository's Cargo.toml already declared GPL-3.0-or-later
before the desktop work; that existing declaration is unchanged. The included
LICENSE-GPL-3.0.txt supports that existing project declaration and records an
available Slint alternative; it is not the selected Slint distribution route.
The corresponding application source and build instructions are included in
the local portable package. This work does not license unrelated user code.

Cargo.lock records exact dependency versions and checksums. `package_desktop.py`
bundles license/notice files from selected registry packages and emits
DEPENDENCY-LICENSES.json with versions, authors, SPDX declarations and upstream
repositories. Some published crates omit their license files; those are identified
by an empty notices list and by the packaging command. Their upstream notices
must be resolved before a public binary release. This local prototype is not a
public installer or a qualified release package.

WinFsp remains a separately installed system prerequisite with its own license.
The desktop app does not install WinFsp, alter PATH, or configure a service.
