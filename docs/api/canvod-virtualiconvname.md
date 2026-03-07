# canvod.virtualiconvname API Reference

Filename convention, mapping, validation, and cataloging.

## Convention

::: canvod.virtualiconvname.convention
    options:
      members:
        - CanVODFilename
        - ReceiverType
        - FileType

## Mapping

::: canvod.virtualiconvname.mapping
    options:
      members:
        - VirtualFile
        - FilenameMapper

## Recipe

::: canvod.virtualiconvname.recipe
    options:
      members:
        - NamingRecipe

## Patterns

::: canvod.virtualiconvname.patterns
    options:
      members:
        - SourcePattern
        - BUILTIN_PATTERNS

## Validation

::: canvod.virtualiconvname.validator
    options:
      members:
        - DataDirectoryValidator
        - ValidationReport

## Catalog

::: canvod.virtualiconvname.catalog
    options:
      members:
        - FilenameCatalog

## Configuration

::: canvod.virtualiconvname.config_models
    options:
      members:
        - SiteNamingConfig
        - ReceiverNamingConfig
        - DirectoryLayout
