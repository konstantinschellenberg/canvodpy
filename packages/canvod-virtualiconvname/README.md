# canvod-naming

Filename convention, mapping engine, and DuckDB-backed catalog for the canVODpy pipeline.

## Features

- **CanVODFilename** — Pydantic model for the canVOD filename convention
- **SourcePattern** — Regex-based pattern matching for diverse file naming schemes
- **FilenameMapper** — Virtual renaming engine that maps physical files to conventional names
- **FilenameCatalog** — DuckDB-backed metadata catalog for file tracking
