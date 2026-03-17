# canvod-utils

## Purpose

The `canvod-utils` package provides configuration management and command-line tooling for the canVODpy ecosystem. It implements a YAML-based configuration system with Pydantic validation, shared date utilities, and CLI shortcuts.

---

## Configuration System

Three YAML files in `config/` control all aspects of a canVODpy deployment:

<div class="grid cards" markdown>

-   :fontawesome-solid-sliders: &nbsp; **`processing.yaml`**

    ---

    Author metadata, NASA CDDIS credentials, auxiliary data agency,
    parallel processing limits, Icechunk compression, store strategies.

-   :fontawesome-solid-map-location-dot: &nbsp; **`sites.yaml`**

    ---

    Research site definitions — data root paths, receiver types,
    directory layout, SCS expansion (`scs_from`), VOD analysis pairs.

-   :fontawesome-solid-broadcast-tower: &nbsp; **`sids.yaml`**

    ---

    Signal ID (SID) filtering — `all`, a named `preset` (e.g. `gps_galileo`),
    or a `custom` list of SIDs to retain.

</div>

User configuration overrides package defaults for any specified values. Unset keys fall back to bundled defaults.

---

## Configuration Files

=== "processing.yaml"

    ```yaml
    metadata:
      author: Your Name
      email: your.email@example.com
      institution: Your Institution

    credentials:
      nasa_earthdata_acc_mail: null  # NASA CDDIS auth (optional)

    aux_data:
      agency: COD
      product_type: final

    processing:
      keep_rnx_vars: [SNR]
      aggregate_glonass_fdma: true
      store_radial_distance: false
      receiver_position_mode: shared     # or per_receiver
      file_pairing: complete             # or paired
      batch_hours: 24
      resource_mode: auto
      # n_max_threads: 8       # manual mode only
      # max_memory_gb: 16      # manual mode only
      # threads_per_worker: 1
      # cpu_affinity: [0, 1, 2, 3]
      # nice_priority: 10

    preprocessing:
      temporal_aggregation:
        enabled: true
        freq: "1min"
        method: mean
      grid_assignment:
        enabled: true
        grid_type: equal_area
        angular_resolution: 2.0

    compression:
      zlib: true
      complevel: 5

    icechunk:
      compression_level: 5
      compression_algorithm: zstd
      inline_threshold: 512
      get_concurrency: 1
      # manifest_preload_enabled: false
      # manifest_preload_max_refs: 100000000
      # manifest_preload_pattern: "epoch|sid"
      chunk_strategies:
        rinex_store:
          epoch: 34560
          sid: -1
        vod_store:
          epoch: 34560
          sid: -1

    storage:
      stores_root_dir: /path/to/your/gnss/stores
      rinex_store_strategy: skip
      vod_store_strategy: overwrite
    ```

=== "sites.yaml"

    ```yaml
    sites:
      rosalia:
        base_dir: /path/to/rosalia
        receivers:
          reference_01:
            type: reference
            directory: 01_reference/01_GNSS/01_raw
            recipe: rosalia_reference.yaml
            reader_format: rinex3
          canopy_01:
            type: canopy
            directory: 02_canopy/01_GNSS/01_raw
            recipe: rosalia_canopy.yaml
            reader_format: auto
            scs_from: null          # use own position
          canopy_02:
            type: canopy
            directory: 02_canopy/02_GNSS/01_raw
        vod_analyses:
          canopy_01_vs_reference_01:
            canopy_receiver: canopy_01
            reference_receiver: reference_01
    ```

=== "sids.yaml"

    ```yaml
    # Keep all signals
    mode: all

    # --- or named preset ---
    # mode: preset
    # preset: gps_galileo

    # --- or explicit list ---
    # mode: custom
    # custom:
    #   - "G01|L1|C"
    #   - "E01|E1|C"
    ```

---

## Loading Configuration

```python
from canvod.utils.config import load_config

config = load_config()

# Access any section
author  = config.processing.metadata.author
agency  = config.processing.aux_data.agency
n_cores = config.processing.processing.n_max_threads
```

!!! tip "Validation at load time"

    All values are validated by Pydantic models. Invalid emails, non-existent paths,
    and out-of-range parameters produce structured error messages immediately
    — not at runtime hours into a long processing run.

---

## CLI Quick Reference

=== "Setup"

    ```bash
    just config-init      # Copy .example templates + recipe files → config/
    just config-edit processing   # Open processing.yaml in $EDITOR
    just config-edit sites        # Open sites.yaml in $EDITOR
    just config-validate  # Validate all configuration files
    just config-show      # Display resolved configuration
    ```

=== "Development"

    ```bash
    just test             # Run full test suite
    just check            # Lint + format + type-check
    just hooks            # Install pre-commit hooks
    just docs             # Serve documentation locally
    just test-coverage    # Tests with coverage report
    just clean            # Remove build artifacts
    ```

=== "Processing"

    ```bash
    just process          # Run full pipeline
    just process-date YYYYDOY     # Process single day
    just process-range START END  # Process date range
    ```
