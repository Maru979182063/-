# Universal Tagger Prompt

Return one `universal_profile` for each span in the same order as the input.
Do not assign final family labels.
Only score the universal routing features.
All numeric scores must be between `0.0` and `1.0`.
Use short, stable labels for `structure_hints`, `logic_relations`, and `position_roles`.
