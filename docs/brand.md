# Corral brand

Corral is a local control plane for agent work.

**Brand promise:** Parallel agents. Isolated branches. Evidence before merge.

## Idea: the fence ledger

The visual system joins two product truths:

- **Fence:** parallel rails represent isolated worktrees; a gate represents
  evidence and operator approval; one clean rail leaves toward the current
  branch.
- **Ledger:** ruled lines and numbered events represent the append-only SQLite
  record that makes a run inspectable and resumable.

Keep the metaphor infrastructural. No western illustration, rope, hats, wood,
or livestock.

## Palette

| Token | Light | Dark | Meaning |
|---|---|---|---|
| paper | `#F3F0E8` | `#141612` | field |
| raised | `#E7E2D7` | `#1D211C` | grouped region |
| ink | `#191A16` | `#ECE8DC` | text and primary structure |
| muted | `#66665E` | `#A3A096` | secondary text |
| rail | `#B6AFA1` | `#4B5048` | inactive structure |
| amber | `#D8890B` | `#F2AA2A` | brand and gates only |
| blue | `#2B63D9` | `#78A4FF` | active work only |
| green | `#1F7A50` | `#57C78B` | verified outcomes only |
| red | `#B8453F` | `#FF8178` | failed outcomes only |

Amber is not decoration. Blue never means branding. Green and red never label
non-terminal states.

## Geometry and type

- Work on an 8 px grid. Use 3 px rails and 6–10 px corner radii.
- Prefer continuous paths, ruled regions, and open space over card grids.
- Use sentence case and lowercase labels.
- Use system sans for prose and system mono for states, commands, IDs, and
  evidence.
- SVG text must remain at least 10 px after README scaling. Supply a stacked
  mobile asset when a wide diagram cannot meet that threshold.
- No external fonts, gradients, filters, shadows, or decorative transparency.

## Mark

Three rails are isolated work. The open bracket is the gate. The single blue
square is an active attempt crossing the boundary. At small sizes, omit all
supporting dots and labels; the rails and open gate must remain legible.
