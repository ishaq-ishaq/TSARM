# Releasing TSARM with a Zenodo DOI

The proposal's Objective iii asks for a release "with a versioned DOI".
Zenodo's GitHub integration archives every GitHub release and gives it a DOI.
It also gives a **concept DOI** that always resolves to the latest version.
Cite the concept DOI in the thesis, and the version DOI in tables that report
results produced by one version.

## Plan

| Version | When | Purpose |
|---|---|---|
| **v0.9.0** | As soon as PR #2 is merged | A DOI exists before the thesis is submitted. This version holds the code for every result obtained so far. |
| **v1.0.0** | When the Wikidata, E2 and E3/E4 results are final | The version the thesis reports. Each table cites it. |
| v1.0.x | After examination, if needed | Corrections. The concept DOI stays the same. |

## One-time setup (yours, on your accounts)

1. Sign in at <https://zenodo.org> with **"Log in with GitHub"**.
2. Open *Account → GitHub* (<https://zenodo.org/account/settings/github/>),
   click *Sync now*, and switch the toggle **on** for `ishaq-ishaq/TSARM`.
   Do this *before* creating the release: Zenodo archives only releases made
   while the toggle is on.
3. Optional: add your ORCID iD. Put it in `.zenodo.json` (`"orcid": "0000-..."`
   under the creator) and in `CITATION.cff` (`orcid: https://orcid.org/0000-...`).

## Each release

```bash
git checkout main && git pull
python3 -m pytest -q                     # all tests pass
python3 scripts/release_check.py 0.9.0   # versions agree, no private files, clean tree on main
git tag -a v0.9.0 -m "TSARM 0.9.0"
git push origin v0.9.0
gh release create v0.9.0 --title "TSARM 0.9.0" --notes-file docs/release_notes/0.9.0.md
```

When Zenodo has archived the release (a minute or two), its GitHub settings
page shows the DOI badge. Then:

1. Put the DOI in `CITATION.cff` (`doi: 10.5281/zenodo.NNNNNNN`). Use the
   concept DOI there, and also on the README badge.
2. Add the badge to the top of `README.md`:
   `[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.NNNNNNN.svg)](https://doi.org/10.5281/zenodo.NNNNNNN)`
3. Fill the DOI placeholders in the thesis chapters.

Before each new release, update the version in `setup.cfg`, `CITATION.cff`
and `.zenodo.json`. `tests/test_release_check.py` fails in CI if the three
disagree.

## What `.zenodo.json` controls

Zenodo reads `.zenodo.json` in preference to `CITATION.cff`. It holds the
title, description, creator and affiliation, licence (MIT) and keywords.
Neither file names anyone except the author. Keep it that way: the repository
is public.

## Licence of the data

The repository contains no Wikidata or DBpedia dumps, only the scripts that
download them. Wikidata is CC0 and DBpedia is CC BY-SA 3.0. The thesis's data
section should name both licences and the dump dates
(`docs/wikidata_data_plan.md`, `scripts/download_dbpedia.sh`).
