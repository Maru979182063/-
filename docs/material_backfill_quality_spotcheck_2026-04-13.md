# Material Backfill Quality Spot Check (2026-04-13)

## 1. Scope

- Spot-check target: the 7 shortage cards that were just filled to `100 stable`
- Sampling method: inspect the latest `5 stable` materials per card from `passage_service.db`
- Additional full-text review: inspect several suspicious samples in full text
- Total samples reviewed: `35`

Cards checked:

- `sentence_fill__opening_topic_intro__abstract`
- `sentence_fill__middle_carry_previous__abstract`
- `sentence_fill__middle_bridge_both_sides__abstract`
- `sentence_order__discourse_logic__abstract`
- `sentence_order__head_tail_lock__abstract`
- `sentence_order__head_tail_logic__abstract`
- `sentence_order__timeline_action_sequence__abstract`

## 2. Overall Verdict

The pool is now **numerically complete**, but **quality is mixed rather than uniformly strong**.

Current read:

- `sentence_order__discourse_logic__abstract`
  - Mostly usable.
- `sentence_order__timeline_action_sequence__abstract`
  - Fairly usable when the material contains explicit temporal progression.
- `sentence_order__head_tail_logic__abstract`
  - Quantity is filled, but many samples are only moderately aligned.
- `sentence_fill__middle_bridge_both_sides__abstract`
  - Mixed; a few look like real bridge units, many are only weakly bridge-like.
- `sentence_fill__middle_carry_previous__abstract`
  - Weakest fill-middle bucket; many samples are ordinary explanation sentences.
- `sentence_fill__opening_topic_intro__abstract`
  - Mixed and slightly polluted; some are true opening/topic lead-ins, some are not.
- `sentence_order__head_tail_lock__abstract`
  - Still weak; many samples do not show a hard head-tail lock.

## 3. Main Bad Patterns

### A. Fill cards still contain ordinary statement sentences

Observed especially in:

- `sentence_fill__middle_carry_previous__abstract`
- `sentence_fill__middle_bridge_both_sides__abstract`

Typical issue:

- the sentence is informative,
- but deleting it would not create a strong blank,
- and it does not clearly function as a true carry or bridge slot.

Examples:

- `mat_6bb2086cce264d46a27be106cffcc943`
  - “明代《天工开物》记载烧炭工艺时提到……”
  - More like factual explanation than a real carry-previous slot.
- `mat_7c29a232df6d48a5b2808ccd0284ba20`
  - “从粗糙的木炭到高效的活性炭……”
  - Reads like a concluding statement, not a strong carry slot.
- `mat_999352ce9c4449f496e2dbb91dd36413`
  - “同时也绘制卷、轴、册……”
  - Too thin; more like additive continuation than a bridge-both-sides slot.

### B. Order cards still contain chunks that are only loosely sortable

Observed especially in:

- `sentence_order__head_tail_lock__abstract`
- `sentence_order__head_tail_logic__abstract`

Typical issue:

- the text block has progression,
- but the opener is not uniquely opener-like,
- or the closer is not strongly closing,
- so it is sortable in a loose sense, not a strong test-grade sense.

Examples:

- `mat_2a85c5e11be6418fb4ba302e7b2c1ac5`
  - A financial-news chunk with topical continuity, but weak head-tail lock.
- `mat_5f92a95094394a6ca96ece110030b72c`
  - Explanatory anatomy block; coherent, but not strongly locked as a head-tail ordering item.
- `mat_1e34409b4833478b87f19ce9ad7f0719`
  - Contains progression, but feels closer to expository grouping than strong head-tail logic.

### C. Source-noise contamination still exists

At least one clearly bad sample is still present:

- `mat_1560e985bd7a4f2585bef9b7c3c9954f`
  - Article title: `中国经济网版权及免责声明`
  - This should not be in `sentence_fill__opening_topic_intro__abstract`.

Also observed:

- `mat_5f92a95094394a6ca96ece110030b72c`
  - Contains `图库版权图片，转载使用可能引发版权纠纷`
  - This is content noise inside an otherwise topical block.

## 4. Card-by-Card Judgment

### `sentence_fill__opening_topic_intro__abstract`

Verdict:

- Semi-usable, but not clean enough.

Why:

- Some samples are valid topic-leading openings.
- Some samples are broad expository openings rather than good blankable intros.
- At least one clearly wrong legal/disclaimer sample exists.

### `sentence_fill__middle_carry_previous__abstract`

Verdict:

- Weak.

Why:

- The bucket has quantity now, but many items are not true dependency-carry slots.
- Many are standalone statements or factual sentences that would not create a meaningful blank.

### `sentence_fill__middle_bridge_both_sides__abstract`

Verdict:

- Mixed, slightly better than carry-previous.

Why:

- Some sentences contain explicit bidirectional markers.
- But quite a few are still just “also / meanwhile” style continuations, not real bridge slots.

### `sentence_order__discourse_logic__abstract`

Verdict:

- Usable.

Why:

- The stronger samples do show discourse progression and local logic.
- This bucket currently looks the most stable among the order cards.

### `sentence_order__head_tail_lock__abstract`

Verdict:

- Weak to mixed.

Why:

- Many samples are coherent blocks, but the head-tail lock is not sharp enough.
- Good enough for loose internal testing, not ideal for stronger downstream use.

### `sentence_order__head_tail_logic__abstract`

Verdict:

- Moderately usable after the refill, but still quality-variable.

Why:

- The refill solved the quantity cliff.
- However, many items are generic expository sequences, not strong head-tail logic chains.

### `sentence_order__timeline_action_sequence__abstract`

Verdict:

- Fairly usable.

Why:

- Temporal or event progression is visible in many samples.
- Still not perfect, but this bucket is closer to “good enough for testing”.

## 5. Final Conclusion

The refill has succeeded on **inventory coverage**, but not on **uniform quality maturity**.

Best current buckets:

- `sentence_order__discourse_logic__abstract`
- `sentence_order__timeline_action_sequence__abstract`
- `sentence_order__head_tail_logic__abstract` (usable but uneven)

Most fragile current buckets:

- `sentence_fill__middle_carry_previous__abstract`
- `sentence_fill__opening_topic_intro__abstract`
- `sentence_order__head_tail_lock__abstract`

Bottom line:

- The pool is now good enough for **internal testing and manual review**.
- It is **not yet clean enough to treat all 7 cards as equally high-quality production stock**.
