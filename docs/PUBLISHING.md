# Publishing KalpakDB

Everything below is verified up to the registry boundary: manifests
package cleanly, the npm tarball and Python sdist/wheel build. The only
missing inputs are registry accounts and tokens.

## crates.io (five crates, dependency order matters)

First publication must follow the dependency graph — a crate can only be
published after everything it depends on exists on crates.io:

```sh
cargo login <token>          # from https://crates.io/settings/tokens

cargo publish -p kalpak-core
cargo publish -p kalpak-proto      # needs protoc on PATH
cargo publish -p kalpak-storage    # depends on core
cargo publish -p kalpak-control    # depends on core
cargo publish -p kalpak-client     # depends on core (+ proto via --features grpc)
cargo publish -p kalpakdb          # depends on all of the above
```

Notes:
- All intra-workspace dependencies carry `version = "X.Y.Z"` alongside
  `path` — `cargo publish` strips the path and uses the version. Keep
  them in lockstep with `workspace.package.version` on every release
  bump (the release scripts already do).
- `cargo package --no-verify -p <crate>` is the offline pre-flight; the
  "no matching package named kalpak-core" error from dependent crates
  disappears once the dependency is live on crates.io.
- crates.io publishes are permanent (yank-only). Name squatting is real:
  publishing `kalpak-core` et al. early (even at 0.x) reserves the names.

## PyPI

```sh
python3 -m venv .venv && .venv/bin/pip install build twine
cd clients/python
../../.venv/bin/python -m build          # dist/kalpakdb-X.Y.Z.{tar.gz,whl}
../../.venv/bin/twine upload dist/*      # needs a PyPI API token
```

## npm

```sh
cd clients/typescript
npm run build
npm publish --access public              # needs `npm login` first
```

The package name `kalpakdb` must be available on each registry; check
before announcing, and prefer publishing all three the same day so the
install instructions in the README go live at once.

## After publishing

1. Update README install sections: `cargo add kalpak-client`,
   `pip install kalpakdb`, `npm install kalpakdb` (replacing the
   from-source paths).
2. Tag the release that the published artifacts were built from.
3. Announce (the README's thesis section is the launch post).
