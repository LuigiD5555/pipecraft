# PipeCraft 1.6 source recovery provenance

The starting point of this 1.7 preview is the reconstructed source for
PipeCraft `1.6.0-alpha.1`. The original 1.6 binary was produced by the Styler
GitHub Actions workflow using the `1.5.0-alpha.1` Rust sources and a
versioned Python patch script.

- Styler build commit: `f35582d9506f53bf7559d74c9751d7d308305557`.
- Source anchor: `62c3fdbee2d1df7e393330812a44542827c16489`.
- Historical patch recipe: `scripts/build_pipecraft_16_temp.py` in the
  historical Styler commit.
- Historical ELF SHA-256: `3fd8e31b5ea60f45e3cb82d808bbe55738118f71da52f2c2576d1c5e70e54ac2`.
- The original recovery patch and notes are retained unmodified under
  `docs/history/` to keep provenance available.

The current tree adds new Rust files and edits. It is **not byte-identical**
to the historical code, and no claim is made that its compiled artifact has
been verified against the historical checksum.
