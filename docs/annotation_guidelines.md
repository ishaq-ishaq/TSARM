# Annotation guidelines for the E3 reference set

Wikidata and DBpedia have no ground-truth rule set. E3's reference set
`R_true` is therefore built by two annotators who judge a pooled, blind sample
of rules. The kit is `scripts/annotation_kit.py`; the method (pooling,
stratified sampling, Cohen's kappa, Wilson intervals, relative recall) is
described in `src/evaluation/annotation.py`.

## 1. Build the sheet

```bash
python3 scripts/annotation_kit.py sheet \
    --tsarm results/wikidata_temporal_rules_h1.json \
    --amie results/amie_wikidata/rules.csv \
    --rdfrules results/rdfrules_wikidata/rdfrules_export.json \
    --sample 300 --fetch-labels --out results/annotation/wikidata_sheet.csv
```

The sheet lists one rule per row, shuffled. A rule found by several systems
appears once. The `systems` column is there for scoring. **Hide or delete it in
the copy you give the annotators** so that they judge blind.

Make two copies, one per annotator. Each annotator fills only their own column
(`judge_a` or `judge_b`) and must not see the other's copy.

## 2. Judge each rule

Read `X => Y` as "entities that are X are usually also Y", or, for fact items,
"entities with property-value X usually also have Y". The `labels` and `links`
columns tell you what each class or property is.

| Mark | Meaning | Examples |
|---|---|---|
| `1` | A **valid, non-trivial** regularity: a domain expert would accept it as usually true, and it says something the ontology does not already state. | `dbo:Athlete => dbo:SoccerPlayer` (most athletes in DBpedia are footballers: a real property of the data) |
| `0` | **Invalid**: false in the domain, or true only because of an extraction artefact. | A rule that links two unrelated classes through a mapping bug |
| `0` | **Trivial**: restates the class hierarchy, or follows from it alone. | `dbo:Person => dbo:Agent` (Person is a subclass of Agent); `dbo:Place AND dbo:PopulatedPlace => dbo:Settlement` when it holds only because Settlement's superclasses are listed |
| `?` | You cannot decide, even after following the links. | Use this sparingly. These rules are left out of the reference set. |

Further rules for judging:

* Judge the rule, not its numbers. Confidence and lifecycle are shown so you
  can see how strong the rule is, but a rule with low confidence can still be
  valid, and a rule with high confidence can be trivial.
* Do not try to guess which system found the rule.
* Write a short reason in `note` for every `0`, and for any `1` you hesitated
  over. The reasons are needed for adjudication and for the thesis.
* Allow roughly 1 minute per rule, so 300 rules take about 5 hours per
  annotator. Work in sessions of about 50 rules.

## 3. Merge and adjudicate

Paste both annotators' columns into one sheet. For every row where `judge_a`
and `judge_b` disagree, the two annotators (or the supervisor) discuss the rule
and write the agreed mark in `final`. Leave `final` empty where they agree.

## 4. Score

```bash
python3 scripts/annotation_kit.py score results/annotation/wikidata_sheet.csv \
    --reference-out results/annotation/wikidata_reference.csv
```

The command reports four things:

* **Cohen's kappa**, computed before adjudication. Report it in the thesis. A
  value of 0.6 or more is usually read as substantial agreement. A lower value
  means the guidelines need tightening, and the batch should be judged again.
* **Precision per system**, with a Wilson 95 % confidence interval.
* **Relative recall** against the pooled reference set.
* **The reference CSV.** `scripts/evaluate_quality.py` and
  `src.evaluation.quality.load_reference_rules` can read it, so E3 can be
  re-run against it.

## Pilot

`results/annotation/dbpedia_class_sheet.csv` holds the 111 rules that
FP-Growth mined from three DBpedia releases using the ontology hierarchy. Use
it to try out these guidelines and to calibrate both annotators before the
Wikidata sheet exists. Many of its rules are trivial by design, because they
were mined without ancestor pruning.
