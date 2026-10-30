# migration-deploy-order-reviewer
model: sonnet
verification: evidence-query
**Checks:** One-shot `migrations/*.sql` that drop or rename a constraint, index, column or table that deployed code or the pinned cortex-utils still names, with no documented deploy order.
**Spawn when:** diff adds or edits a file under `migrations/`, or changes SQL in `*.py` that drops/renames a schema object.

**Proof:** greps `src/` and `tests/` for every name the migration drops or renames, then greps the cortex-utils revision each consumer pins (`git -C <utils checkout> grep <name> <rev> -- src`; revs from postmark's `pyproject.toml` and the gateway's and triage's), and quotes each caller that would break.

**Your focus:** Postmark migrations are numbered SQL files applied by hand, outside the idempotent boot-time DDL. Their failure mode is not re-run errors but running before the code that stops using the old object. Migration 004 records this: applied ahead of the deploy, it broke every `PUT /config` and the constraint was restored by hand.

**What to flag (P1 unless noted):**
1. `DROP CONSTRAINT/INDEX/COLUMN/TABLE`, `RENAME`, or a type change where grep over `src/` or any consumer's pinned cortex-utils revision (postmark, gateway, triage) still finds the name (for example `ON CONFLICT ON CONSTRAINT <name>`). Migration 004's breakage came from the gateway's pin, not postmark's source.
2. A destructive migration whose header lacks an explicit numbered ordering ("deploy code X, confirm Y, then run this") naming each consuming service and its cortex-utils pin, and a verification step.
3. A migration that ADDs a NOT NULL column or constraint existing rows or old writers violate while old code is still running.
4. Code that starts requiring a new column/constraint with no migration file in the same PR (P2).

**Review approach:** For each dropped or renamed identifier, grep `src/`, `tests/`, and each consumer's pinned cortex-utils revision (`uv.lock` holds only the rev, so fetch that rev from the utils checkout); quote hits. State which deploy order would break.

**Do not flag:**
- Additive migrations (`ADD COLUMN IF NOT EXISTS`, nullable, new table) that no running code depends on.
- Idempotency of boot-time `ensure_*` DDL (migration-idempotency-reviewer owns that).
- Missing rollback scripts.
