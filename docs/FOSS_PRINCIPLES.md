# Free and Open Source Software (FOSS) Principles

## Overview

**canvodpy** is developed as Free and Open Source Software (FOSS) following the principles established by the Free Software Foundation<sup>[1](#ref1)</sup> and endorsed by the Open Source Initiative.<sup>[2](#ref2)</sup> This commitment ensures the software remains freely accessible, modifiable, and redistributable, enabling transparent scientific research and fostering collaborative development within the GNSS and atmospheric science communities.

## Definitions and Distinctions

### Free Software

Free software, as defined by the Free Software Foundation, guarantees users four essential freedoms:<sup>[1](#ref1)</sup>

0. **Freedom to run** the program for any purpose
1. **Freedom to study** how the program works and change it
2. **Freedom to redistribute** copies
3. **Freedom to distribute** modified versions

The term "free" refers to liberty (*libre*), not price (*gratis*), emphasizing user autonomy over software control.

### Open Source Software

Open source software, as characterized by the Open Source Initiative, emphasizes development methodology and source code availability.<sup>[2](#ref2)</sup> The Open Source Definition requires:

- Free redistribution without restrictions
- Source code availability and modification rights
- Derived works and modifications permitted
- No discrimination against persons, groups, or fields of endeavor
- License distribution without requiring additional agreements
- Technology neutrality

### Relationship and Convergence

While historically distinguished by philosophical emphasis—free software prioritizing user freedom, open source emphasizing practical benefits—the vast majority of licenses satisfy both definitions.<sup>[3](#ref3)</sup> Modern practice increasingly treats these as complementary perspectives on the same software licensing paradigm, hence the unified term "FOSS" (Free and Open Source Software) or "FLOSS" (Free/Libre and Open Source Software).

## License: Apache License 2.0

**canvodpy** is licensed under the **Apache License, Version 2.0**,<sup>[4](#ref4)</sup> a permissive FOSS license approved by both the Free Software Foundation<sup>[5](#ref5)</sup> and Open Source Initiative.<sup>[6](#ref6)</sup>

### Key Provisions

The Apache 2.0 license provides:

- **Permissive use rights**: Commercial and non-commercial use without restrictions
- **Modification freedom**: Unlimited source code modification and distribution
- **Patent grant**: Explicit patent license from contributors to users
- **Trademark protection**: Project name and marks remain protected
- **Attribution requirements**: Copyright and license notices must be retained
- **No copyleft**: Modified versions may use different licenses

### Rationale for Selection

Apache 2.0 was selected for several reasons relevant to scientific software:

1. **Patent protection**: Explicit patent grants prevent contributor patent claims against users<sup>[7](#ref7)</sup>
2. **Enterprise compatibility**: Widely accepted in academic and commercial settings<sup>[8](#ref8)</sup>
3. **Clear terms**: Well-documented license reduces legal ambiguity<sup>[4](#ref4)</sup>
4. **Permissive integration**: Compatible with most other FOSS licenses, enabling ecosystem integration

## FOSS Compliance Frameworks

### FAIR Software Principles

The FAIR principles—originally developed for research data<sup>[19](#ref19)</sup>—have been adapted for research software<sup>[20](#ref20)</sup> to enhance:

- **Findability**: Persistent identifiers and metadata enable software discovery
- **Accessibility**: Open access and clear licenses facilitate software reuse
- **Interoperability**: Standards-based design supports integration
- **Reusability**: Documentation and licensing enable legitimate reuse

**canvodpy** implements FAIR software through:

- GitHub repository with persistent URL
- Apache 2.0 open source license
- Zenodo DOI: [10.5281/zenodo.18636775](https://doi.org/10.5281/zenodo.18636775)
- Citation metadata (CITATION.cff, codemeta.json)
- OpenSSF Best Practices certification
- Automated FAIR compliance checking

**Status**: 4/5 FAIR recommendations met (pending PyPI registration for v1.0)

**Reference**: [FAIR Software Compliance Summary](FAIR_IMPLEMENTATION_SUMMARY.md)

### OpenSSF Best Practices

The Open Source Security Foundation (OpenSSF) Best Practices badge program<sup>[21](#ref21)</sup> establishes security and quality criteria for FOSS projects. The program promotes:

- **Security awareness**: Vulnerability reporting and response procedures
- **Code quality**: Static analysis, testing, and code review practices
- **Development transparency**: Public version control and issue tracking
- **Release integrity**: Signed releases and supply chain security

**canvodpy** earned the OpenSSF Best Practices badge (passing level) by demonstrating:

- Security policy with coordinated disclosure (SECURITY.md)
- Static analysis (ruff, ty) and dynamic testing (pytest)
- Public development on GitHub with issue tracking
- Automated security scanning (OpenSSF Scorecard)
- Reproducible builds with locked dependencies

**Badge**: [OpenSSF Best Practices Project 12329](https://www.bestpractices.dev/projects/12329)

**Reference**: [OpenSSF Badge Implementation Guide](OPENSSF_BADGE_GUIDE.md)

### Software Heritage Archival

Software Heritage<sup>[22](#ref22)</sup> provides permanent archival of public source code, ensuring long-term preservation independently of hosting platform stability. As a public GitHub repository, **canvodpy** is automatically archived by Software Heritage, contributing to the permanent scholarly record.

## Community and Governance

### Contribution Model

**canvodpy** welcomes contributions following established FOSS development patterns:

- **Issue tracking**: Bug reports and feature requests on GitHub Issues
- **Pull requests**: Code contributions via GitHub Pull Requests
- **Code review**: Maintainer review before integration
- **CI validation**: Automated testing verifies changes
- **License agreement**: Contributors grant rights under Apache 2.0

**Reference**: [CONTRIBUTING.md](CONTRIBUTING.md)

### Maintenance and Support

The project is actively maintained with:

- Regular dependency updates and security patches
- Responsive issue triage (target: 7 days for initial response)
- Semantic versioning for API stability
- Long-term support considerations for published research

### Attribution and Citation

Academic users should cite **canvodpy** using the provided citation metadata:

- **Citation file**: [CITATION.cff](../CITATION.cff)
- **Zenodo record**: [10.5281/zenodo.18636775](https://doi.org/10.5281/zenodo.18636775)

Proper citation ensures software developers receive academic credit, addressing a key sustainability challenge for research software.<sup>[17](#ref17)</sup>

## Future Directions

### Planned Enhancements

- **PyPI publication**: Complete 5/5 FAIR compliance with package registry distribution
- **Silver badge**: Pursue OpenSSF Best Practices silver level certification
- **Community growth**: Expand contributor base and governance structures
- **Sustainability**: Explore funding mechanisms for long-term maintenance

### Commitments

**canvodpy** commits to:

- Maintaining open source availability under Apache 2.0 or compatible license
- Preserving public development history and decision documentation
- Supporting reproducible research through stable, well-documented releases
- Engaging with the scientific software community through workshops and publications

## References

<a name="ref1"></a>
1. Free Software Foundation. "What is Free Software?" [https://www.gnu.org/philosophy/free-sw.html](https://www.gnu.org/philosophy/free-sw.html)

<a name="ref2"></a>
2. Open Source Initiative. "The Open Source Definition." [https://opensource.org/osd](https://opensource.org/osd)

<a name="ref3"></a>
3. Stallman, R. (2016). "Why Open Source Misses the Point of Free Software." In *Free Software, Free Society: Selected Essays of Richard M. Stallman, 3rd Edition*. GNU Press.

<a name="ref4"></a>
4. Apache Software Foundation. "Apache License, Version 2.0." (2004). [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

<a name="ref5"></a>
5. Free Software Foundation. "Various Licenses and Comments about Them." [https://www.gnu.org/licenses/license-list.html#apache2](https://www.gnu.org/licenses/license-list.html#apache2)

<a name="ref6"></a>
6. Open Source Initiative. "Apache License 2.0." [https://opensource.org/licenses/Apache-2.0](https://opensource.org/licenses/Apache-2.0)

<a name="ref7"></a>
7. Välimäki, M., & Oksanen, V. (2007). "The Impact of the Apache 2.0 License on Free and Open Source Software." *International Free and Open Source Software Law Review*, 1(1), 49-57.

<a name="ref8"></a>
8. GitHub. "The State of Open Source Software 2024." [https://github.com/features/security](https://github.com/features/security)

<a name="ref9"></a>
9. Stodden, V. (2009). "The Legal Framework for Reproducible Scientific Research: Licensing and Copyright." *Computing in Science & Engineering*, 11(1), 35-40. [https://doi.org/10.1109/MCSE.2009.19](https://doi.org/10.1109/MCSE.2009.19)

<a name="ref10"></a>
10. Ince, D. C., Hatton, L., & Graham-Cumming, J. (2012). "The Case for Open Computer Programs." *Nature*, 482(7386), 485-488. [https://doi.org/10.1038/nature10836](https://doi.org/10.1038/nature10836)

<a name="ref11"></a>
11. Morin, A., et al. (2012). "Shining Light into Black Boxes." *Science*, 336(6078), 159-160. [https://doi.org/10.1126/science.1218263](https://doi.org/10.1126/science.1218263)

<a name="ref12"></a>
12. Barnes, N. (2010). "Publish Your Computer Code: It is Good Enough." *Nature*, 467(7317), 753. [https://doi.org/10.1038/467753a](https://doi.org/10.1038/467753a)

<a name="ref13"></a>
13. Raymond, E. S. (1999). "The Cathedral and the Bazaar." *Knowledge, Technology & Policy*, 12(3), 23-49.

<a name="ref14"></a>
14. Perez-Riverol, Y., et al. (2016). "Ten Simple Rules for Taking Advantage of Git and GitHub." *PLOS Computational Biology*, 12(7), e1004947. [https://doi.org/10.1371/journal.pcbi.1004947](https://doi.org/10.1371/journal.pcbi.1004947)

<a name="ref15"></a>
15. Lamprecht, A. L., et al. (2020). "Towards FAIR Principles for Research Software." *Data Science*, 3(1), 37-59. [https://doi.org/10.3233/DS-190026](https://doi.org/10.3233/DS-190026)

<a name="ref16"></a>
16. Eghbal, N. (2016). "Roads and Bridges: The Unseen Labor Behind Our Digital Infrastructure." Ford Foundation. [https://www.fordfoundation.org/work/learning/research-reports/roads-and-bridges-the-unseen-labor-behind-our-digital-infrastructure/](https://www.fordfoundation.org/work/learning/research-reports/roads-and-bridges-the-unseen-labor-behind-our-digital-infrastructure/)

<a name="ref17"></a>
17. Katz, D. S., et al. (2018). "The Importance of Software Citation." *F1000Research*, 7, 1926. [https://doi.org/10.12688/f1000research.16800.1](https://doi.org/10.12688/f1000research.16800.1)

<a name="ref18"></a>
18. Wilson, G., et al. (2014). "Best Practices for Scientific Computing." *PLOS Biology*, 12(1), e1001745. [https://doi.org/10.1371/journal.pbio.1001745](https://doi.org/10.1371/journal.pbio.1001745)

<a name="ref19"></a>
19. Wilkinson, M. D., et al. (2016). "The FAIR Guiding Principles for Scientific Data Management and Stewardship." *Scientific Data*, 3, 160018. [https://doi.org/10.1038/sdata.2016.18](https://doi.org/10.1038/sdata.2016.18)

<a name="ref20"></a>
20. Barker, M., et al. (2022). "Introducing the FAIR Principles for Research Software." *Scientific Data*, 9, 622. [https://doi.org/10.1038/s41597-022-01710-x](https://doi.org/10.1038/s41597-022-01710-x)

<a name="ref21"></a>
21. Wheeler, D. A. (2015). "The Linux Foundation Core Infrastructure Initiative Best Practices Badge." [https://bestpractices.coreinfrastructure.org/](https://bestpractices.coreinfrastructure.org/) (now [https://www.bestpractices.dev/](https://www.bestpractices.dev/))

<a name="ref22"></a>
22. Di Cosmo, R., & Zacchiroli, S. (2017). "Software Heritage: Why and How to Preserve Software Source Code." *iPRES 2017: 14th International Conference on Digital Preservation*. [https://hal.archives-ouvertes.fr/hal-01590958](https://hal.archives-ouvertes.fr/hal-01590958)

---

## Additional Resources

- **FAIR Software**: [fair-software.eu](https://fair-software.eu/)
- **OpenSSF**: [openssf.org](https://openssf.org/)
- **Software Heritage**: [softwareheritage.org](https://www.softwareheritage.org/)
- **FORCE11 Software Citation Principles**: [force11.org/software-citation-principles](https://force11.org/software-citation-principles)
- **Journal of Open Source Software**: [joss.theoj.org](https://joss.theoj.org/)
- **Research Software Alliance**: [researchsoft.org](https://www.researchsoft.org/)

---

*This document is maintained as part of canvodpy's commitment to transparent, reproducible, and collaborative scientific software development.*
