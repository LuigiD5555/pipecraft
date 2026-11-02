# PipeCraft 1.6.0-alpha.1: fuentes recuperadas del historial de Styler

## Origen comprobado

- Base: ZIP PipeCraft 1.5.0-alpha.1 aportado por el usuario. Los blobs Git de los archivos originales modificados coinciden con el commit histórico `62c3fdbee2d1df7e393330812a44542827c16489` en `LuigiD5555/styler`.
- Transformaciones: script histórico `scripts/build_pipecraft_16_temp.py` en el commit `f35582d9506f53bf7559d74c9751d7d308305557`, aplicado a la base por una reproducción local de sus inserciones.
- Compilación histórica GitHub Actions: ejecución `32326726645` (20 agosto 2026), trabajo en rama `build/pipecraft-1.6.0-alpha.1-clean`, artefacto `9391642629`.
- Ejecutable histórico observado y confirmado dentro de Styler 0.14.0-alpha.17, SHA256: `3fd8e31b5ea60f45e3cb82d808bbe55738118f71da52f2c2576d1c5e70e54ac2`.

## Cambios reproducidos

1. Cambia versión a `1.6.0-alpha.1`.
2. Convierte el parser de pipelines YAML en un parser reusable de `serde_yaml::Value` (`load_pipeline_value`), manteniendo la carga desde archivo.
3. Añade `SubmitSpec`, `ValidateSpec` y `PlanSpec` al protocolo serde del servicio IPC.
4. Añade handlers al `serve`, capacidades anunciadas en `ping` y límite de 8 MiB por request.
5. Guarda duraderamente `pipeline.snapshot.yaml` en el directorio de cada ejecución mediante archivo temporal, `sync_all` y `rename`.
6. Añade `select_requested_order` para `from_step`/`only` y dependencia `serde_yaml` en crate service.

## Recompilar

Requiere toolchain Rust/Cargo (histórico: rustc 1.97.1). Desde esta carpeta:

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo test --workspace -- --test-threads=1
cargo build --release -p pipecraft-cli
./target/release/pipecraft --version
```

El empaquetado histórico producía `artifact/pipecraft` y `artifact/pipecraft.sha256`.

## Alcance de la verificación

- El binario histórico 1.6 se ejecutó nuevamente el 2 nov 2026: `ping`, `validate_spec`, `plan_spec`, `submit_spec` y `status` respondieron por el socket real `pipecraft.ipc/v1`.
- En este entorno NO hay `cargo`/`rustc`: todavía NO se compiló este ZIP reconstruido, y por ello NO afirmamos que su salida sea bit a bit igual al ELF histórico.
- Se reprodujeron las inserciones de código Rust del script original. Para comparar el script autoritativo, usar su enlace de commit en `RECOVERY_PROVENANCE.json`.
- La versión 1.6 recuperada NO contiene todavía la política de fairness/admisión adaptativa del plan Extracta; ésta es la base real sobre la que habría que implementar esa ampliación.
