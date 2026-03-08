# Satellite Metadata Sources for VOD

Date: 2026-03-08
Status: Partial (SNX implemented, others planned)

## Implemented

### IGS Satellite Metadata SINEX (`igs_satellite_metadata.snx`)

- **URL**: `https://files.igs.org/pub/station/general/igs_satellite_metadata.snx`
- **Module**: `canvod.readers.gnss_specs.satellite_catalog.SatelliteCatalog`
- **Coverage**: GPS, GLONASS, Galileo, BeiDou, QZSS, IRNSS (291 SVNs)
- **Data blocks**:
  - SATELLITE/IDENTIFIER — SVN, COSPAR ID, SatCat, block type, launch date
  - SATELLITE/PRN — time-aware PRN↔SVN mapping (374 assignments)
  - SATELLITE/TX_POWER — transmit power in Watts per SVN (20–550W range)
  - SATELLITE/MASS — satellite mass in kg
  - SATELLITE/FREQUENCY_CHANNEL — GLONASS frequency channel numbers
  - SATELLITE/PLANE — orbital plane and slot assignments

**Key use cases**:
- Detect PRN reassignments (SVN change behind a PRN) that affect SNR characteristics
- TX power normalization across satellite blocks
- GLONASS frequency channel identification

---

## Planned

### ANTEX Files (`.atx`) — Antenna Gain Patterns

- **Source**: IGS publishes `igs20.atx` (satellite + receiver antenna models)
- **URL**: `https://files.igs.org/pub/station/general/igs20.atx`
- **Contains**:
  - Phase Center Variations (PCVs) per satellite SVN and antenna type
  - Elevation-dependent gain patterns (nadir angle 0–17°)
  - Frequency-specific corrections (L1, L2, L5, etc.)
- **Relevance to VOD**:
  - Satellite antenna gain varies with nadir angle (= function of elevation at receiver)
  - For zeroth-order VOD (tau-omega), gain pattern differences may cancel between
    canopy and reference receivers viewing the same satellite at similar elevations
  - For higher-order retrievals or multi-frequency approaches, gain patterns matter
  - Different satellite blocks have different antenna designs → different gain patterns
- **Format**: ANTEX 1.4 — fixed-width text, one antenna per block, PCVs as function of
  nadir angle in 0.5° steps
- **Priority**: Medium — impacts accuracy of VOD retrievals, especially at low elevations

### EIRP Lookup Tables — Effective Isotropic Radiated Power

- **Source**: System ICDs (Interface Control Documents) per constellation
  - GPS: IS-GPS-200 (L1 C/A, L2C, L5), IS-GPS-705 (L5), IS-GPS-800 (L1C)
  - Galileo: OS-SIS-ICD
  - BeiDou: BDS-SIS-ICD
  - GLONASS: ICD-GLONASS
- **Contains**:
  - Minimum guaranteed EIRP per signal and satellite block
  - Some ICDs provide typical EIRP as function of off-boresight angle
- **Relevance to VOD**:
  - EIRP = TX power × antenna gain → the actual signal power toward the receiver
  - Combined with free-space path loss, gives expected C/N0 in clear-sky conditions
  - Deviations from expected C/N0 = attenuation by vegetation (= VOD signal)
- **Challenge**: ICD values are minimum specs, not measured per-satellite values.
  Real EIRP varies by SVN, frequency, and nadir angle.
- **Priority**: Low — the tau-omega model uses SNR ratios (canopy/reference), so
  absolute EIRP cancels. Only relevant for absolute calibration approaches.

### Operational Status / Health

- **Source**: No single authoritative real-time source
- **Options**:
  - NANU (Notice Advisory to NAVSTAR Users) — GPS only, text-based advisories
  - RINEX NAV health flags — per-satellite, per-epoch, from broadcast navigation message
  - SP3/CLK presence — if an agency tracks a satellite, it's likely healthy
    (but agencies track different subsets — **not reliable as sole indicator**)
- **Relevance to VOD**:
  - Unhealthy/decommissioned satellites should be excluded from processing
  - Currently handled implicitly: if no signal is received, no data exists
- **Priority**: Low — practically handled by data availability

---

## Design Note: Why Capture Everything

Even when the zeroth-order tau-omega model assumes certain effects cancel between
canopy and reference receivers (e.g., TX power, antenna gain patterns), it is better
to be aware of and have access to this metadata:

1. **Quality control** — unexpected SNR anomalies can be traced to satellite events
   (power changes, antenna reconfiguration, PRN reassignment)
2. **Higher-order models** — future VOD algorithms may account for these effects
3. **Multi-site comparison** — when comparing VOD across sites at different latitudes,
   elevation angle distributions differ → gain pattern effects don't fully cancel
4. **Diagnostics** — TX power changes mid-dataset can cause apparent VOD jumps
