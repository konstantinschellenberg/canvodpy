# canvod.utils API Reference

Shared utilities: configuration management, date handling, and CLI tools.

## Configuration

::: canvod.utils.config
    options:
      members:
        - load_config
        - CanvodConfig
        - MetadataConfig
        - ProcessingConfig
        - SiteConfig
        - SitesConfig
        - SidsConfig
        - ProcessingParams
        - ReceiverConfig
        - PreprocessingConfig
        - TemporalAggregationConfig
        - GridAssignmentConfig

## Tools

::: canvod.utils.tools
    options:
      members:
        - YYYYDOY
        - YYDOY
        - get_gps_week_from_filename
        - gpsweekday
        - isfloat
        - get_version_from_pyproject
