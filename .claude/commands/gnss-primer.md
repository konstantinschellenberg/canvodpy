Explain the GNSS-Transmissometry (GNSS-T) method for measuring Vegetation Optical Depth (VOD) to a newcomer. Cover:

1. **What is GNSS-T?** A ground-based remote sensing technique using commercial GNSS receivers. One antenna below the canopy, one with open sky view. Signal attenuation through vegetation → VOD.

2. **Key observables:** SNR (Signal-to-Noise Ratio in dB-Hz), pseudorange, carrier phase. VOD primarily uses SNR.

3. **Signal ID (SID) system:** `"{constellation}{prn}|{band}|{attribute}"` (e.g. `"G01|L1|C"`). One satellite (PRN) produces multiple SIDs across frequency bands and observable types.

4. **Dataset structure:** `(epoch, sid)` dimensions. `epoch` = timestamp, `sid` = unique signal identifier. Every dataset must have a `"File Hash"` attribute.

5. **Pipeline stages:** File discovery → Reader (RINEX/SBF → xarray) → Ephemeris augmentation (satellite positions) → Grid assignment (hemisphere cells) → VOD calculation (Tau-Omega model) → Store (Icechunk/Zarr)

6. **Constellations:** GPS (G), Galileo (E), GLONASS (R), BeiDou (C), SBAS (S), IRNSS (I), QZSS (J)

Keep it concise and practical — this is onboarding context, not a textbook. Reference relevant package CLAUDE.md files for deeper dives.
