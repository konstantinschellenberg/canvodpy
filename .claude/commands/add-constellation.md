Guide adding a new GNSS constellation to canvod-readers. Follow this template:

## Steps

1. **Create constellation class** in `packages/canvod-readers/src/canvod/readers/gnss_specs/`:
   ```python
   from canvod.readers.gnss_specs.base import ConstellationBase

   class MyConstellation(ConstellationBase):
       SYSTEM_PREFIX = "X"  # single-char RINEX system identifier
       SYSTEM_NAME = "MySystem"

       # Frequency table: band → Hz
       FREQUENCIES = {
           "L1": 1575.42e6,
           "L5": 1176.45e6,
       }

       # Valid PRN range
       MIN_PRN = 1
       MAX_PRN = 50

       def get_default_svs(self) -> list[str]:
           return [f"X{i:02d}" for i in range(self.MIN_PRN, self.MAX_PRN + 1)]
   ```

2. **Register in `__init__.py`** of `gnss_specs/`

3. **Add to RINEX observation type mapping** in `rinex/rnxv3_obs.py` — map RINEX obs codes to SID attributes

4. **SatelliteCatalog integration** — if the constellation is in the IGS SINEX file, `update_svs_from_catalog(on_date)` will work automatically via the `SYSTEM_PREFIX`

5. **Add frequency band definitions** to signal tables if needed

6. **Write tests:**
   - Unit test for PRN range, frequency lookup
   - Integration test with real observation file if available

## Existing constellations for reference

| Class | Prefix | Module |
|---|---|---|
| `GPS` | G | `gps.py` |
| `GALILEO` | E | `galileo.py` |
| `GLONASS` | R | `glonass.py` (has FDMA frequency channels) |
| `BEIDOU` | C | `beidou.py` |
| `SBAS` | S | `sbas.py` |
| `IRNSS` | I | `irnss.py` |
| `QZSS` | J | `qzss.py` |

## GLONASS note

GLONASS uses FDMA (frequency-division), so each satellite has a unique frequency offset. The `GLONASS` class handles this via `SatelliteCatalog.glonass_channel()`. If the new constellation also uses FDMA, follow the GLONASS pattern.
