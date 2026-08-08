# Corral brand

Corral is a local control plane for agent work.

**Brand promise:** Parallel agents. Isolated branches. Evidence before merge.

## Idea: proof ribbons

Corral's brand is a moving system, not a standalone pictorial logo:

- **Ribbons:** broad blue paths represent isolated work in flight. Their
  punched counterforms differ because each attempt produces distinct work.
- **Gate:** one monumental amber aperture represents evidence and operator
  judgment. Amber is the moment work must prove itself.
- **Output:** one green band represents accepted work moving toward the current
  branch.
- **Ledger:** a continuous ink band collects stamped impressions. Earlier
  marks remain when an attempt loops back and resumes.

The result should feel like an art-directed screenprint: bold at thumbnail
size, precise without looking like a flowchart, and recognizable without the
name inside the image.

## Logo: proof portal

The standalone mark compresses the system into one gesture: a cobalt control
loop opens at an amber proof aperture, then lands as a short green band. Its
open counterform creates a quiet `C` without adding text to the image.

- Keep the mark upright, text-free, and surrounded by generous clear space.
- Use it above or beside a native `Corral` wordmark; never bake the wordmark
  into the asset.
- Do not recolor individual parts: blue is work, amber is proof, and green is
  the accepted result.
- At small sizes, use the mark alone. Do not add grain, outlines, containers,
  shadows, or secondary symbols.

## Palette

| Token | Light | Dark | Meaning |
|---|---|---|---|
| paper | `#F3F0E8` | `#141612` | field |
| ink | `#191A16` | `#ECE8DC` | text and primary structure |
| muted | `#66665E` | `#A3A096` | secondary text |
| amber | `#D8890B` | `#F2AA2A` | brand and gates only |
| blue | `#2B63D9` | `#78A4FF` | active work only |
| green | `#1F7A50` | `#57C78B` | verified outcomes only |
| red | `#B8453F` | `#FF8178` | failed outcomes only |

Amber is not decoration. Blue never means branding. Green and red never label
non-terminal states.

## Illustration rules

- Use flat 2D shapes, broad ribbons, hard crop edges, and asymmetric rhythm.
- Keep banner art text-free. Product name, claims, and metrics stay as native
  Markdown where they remain searchable, selectable, and accessible.
- Use only five inks in brand art: paper, ink, blue, amber, and green. Red is
  reserved for technical failure states outside campaign art.
- Allow light paper tooth and screenprint grain. Do not use gradients, shadows,
  3D extrusion, realistic materials, neon, glass, or generic SaaS blobs.
- Avoid literal arrows, flowchart nodes, interfaces, robots, ranch imagery,
  fences, livestock, hats, and rope.
- Desktop banners use a `3:1` crop. Mobile hero art uses a compact horizontal
  `4:3` composition rather than shrinking desktop geometry.
- Supply light and dark assets with identical composition and semantic color
  roles.

## Type and technical diagrams

- The primary identifier pairs the proof-portal mark with the native `Corral`
  wordmark set by the host interface; text is never baked into art.
- Use system sans for prose and system mono for states, commands, IDs, and
  evidence.
- Keep technical diagrams subordinate to campaign art. Put dense diagrams
  behind disclosure when the surrounding prose already explains the model.
- SVG text must remain at least 10 px after README scaling. Supply a stacked
  mobile asset when a wide diagram cannot meet that threshold.
- No external fonts, filters, or decorative transparency.

## Asset set

| Asset | Role |
|---|---|
| `logo.png` | transparent standalone proof-portal mark |
| `hero-light.png`, `hero-dark.png` | primary `3:1` campaign banner |
| `hero-light-mobile.png`, `hero-dark-mobile.png` | recomposed `4:3` hero for narrow README columns |
| `ledger-light.png`, `ledger-dark.png` | durability and retry section banner |
| `tui.svg`, `tui-mobile.svg` | deterministic product-state illustration |

Use the native `Corral` name beside or below the standalone mark. Campaign
banners remain text-free and may appear independently when the page already
names the product.
