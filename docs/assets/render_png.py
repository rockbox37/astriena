#!/usr/bin/env python3
"""Regenerate Astriena's SVG and PNG brand assets.

The committed Cinzel Decorative font is the unmodified Google Fonts release.
Install tooling in a temporary environment; no Python packages belong in the
Go module:

    python3 -m venv /tmp/astriena-brand
    /tmp/astriena-brand/bin/pip install fonttools==4.64.0 cairosvg==2.9.1
    /tmp/astriena-brand/bin/python docs/assets/render_png.py
"""

from __future__ import annotations

from pathlib import Path

import cairosvg
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont

BG = "#0B1220"
SURFACE = "#141C2E"
STAR = "#E8D5A3"
CORE = "#F7F1E1"
DIM = "#5C6B84"
TEXT = "#E8EEF7"
MUTED = "#9AA8BC"

FIELD = [
    (6, 9, .28), (14, 6, .38), (25, 8, .24), (36, 5, .31),
    (49, 7, .25), (58, 12, .36), (4, 26, .23), (8, 39, .34),
    (5, 51, .25), (13, 58, .30), (29, 58, .24), (41, 60, .33),
    (55, 56, .27), (60, 45, .35), (58, 35, .22), (61, 22, .29),
    (12, 27, .22), (22, 28, .30), (34, 27, .21), (47, 20, .24),
    (50, 38, .32), (44, 50, .22), (31, 51, .27), (11, 46, .20),
    (27, 14, .19), (39, 38, .24), (54, 15, .18), (35, 56, .18),
]
STARS = [
    (18, 16, .88), (39, 31, .74), (25, 45, .68), (54, 27, .62),
    (43, 12, .56), (14, 34, .51), (31, 19, .46), (18, 54, .43),
    (9, 21, .39), (48, 46, .36), (34, 42, .33), (52, 16, .30),
]
STAR_CORES = [(18, 16, .27), (39, 31, .22), (25, 45, .19), (54, 27, .17)]
FIELD_16 = [
    (2.5, 3, .28), (12.7, 2.4, .25), (2.2, 10.8, .22),
    (13.6, 11.5, .30), (4.4, 13.7, .24), (11.4, 14, .20),
]
SOCIAL_FIELD = [
    (80, 70, 1.2), (160, 140, .9), (240, 48, 1.1), (420, 90, .8),
    (980, 70, 1.2), (1100, 120, .9), (1180, 200, 1.1), (1040, 520, 1),
    (200, 540, 1.1), (60, 400, .8), (720, 80, .7), (860, 560, .9),
    (1240, 400, .8), (500, 580, .7), (620, 40, .7), (780, 520, .8),
    (1140, 48, .6), (320, 600, .7), (40, 220, .65), (1260, 280, .7),
    (880, 96, .55), (560, 600, .6), (980, 300, .7), (160, 300, .5),
]


def circles(points: list[tuple[float, float, float]], fill: str) -> str:
    return "".join(
        f'<circle cx="{x:g}" cy="{y:g}" r="{r:g}"/>' for x, y, r in points
    )


class Outliner:
    """Convert glyphs from the vendored TTF into SVG path data."""

    def __init__(self, path: Path) -> None:
        self.font = TTFont(path)
        self.glyphs = self.font.getGlyphSet()
        self.cmap = self.font.getBestCmap()
        self.hmtx = self.font["hmtx"].metrics
        self.units = self.font["head"].unitsPerEm

    def glyph_name(self, character: str) -> str:
        return self.cmap[ord(character)]

    def glyph_path(
        self, character: str, x: float, baseline: float, scale: float
    ) -> str:
        pen = SVGPathPen(
            self.glyphs,
            ntos=lambda value: f"{value:.3f}".rstrip("0").rstrip("."),
        )
        transformed = TransformPen(pen, (scale, 0, 0, -scale, x, baseline))
        self.glyphs[self.glyph_name(character)].draw(transformed)
        return pen.getCommands()

    def text_paths(
        self, text: str, x: float, baseline: float, size: float,
        tracking: float = 0,
    ) -> tuple[list[str], float]:
        scale = size / self.units
        paths: list[str] = []
        cursor = x
        for character in text:
            name = self.glyph_name(character)
            paths.append(self.glyph_path(character, cursor, baseline, scale))
            cursor += self.hmtx[name][0] * scale + tracking
        return paths, cursor


def svg_header(view_box: str, title: str, desc: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view_box}" '
        'role="img" aria-labelledby="title desc">\n'
        f'  <title id="title">{title}</title>\n'
        f'  <desc id="desc">{desc}</desc>\n'
    )


def mark_svg(outliner: Outliner, compact: bool = False) -> str:
    size = 16 if compact else 64
    field = FIELD_16 if compact else FIELD
    radius = 3.5 if compact else 14
    if compact:
        a_path = outliner.glyph_path("A", 2.5, 10.8, .012)
    else:
        a_path = outliner.glyph_path("A", 10.5, 44.2, .046)
    star_groups = ""
    if not compact:
        star_groups = (
            f'  <g fill="{STAR}">{circles(STARS, STAR)}</g>\n'
            f'  <g fill="{CORE}">{circles(STAR_CORES, CORE)}</g>\n'
        )
    return (
        svg_header(
            f"0 0 {size} {size}",
            "Astriena",
            "A Cinzel Decorative capital A over an irregular star field.",
        )
        + (
            '  <defs><radialGradient id="sky" cx="34%" cy="27%" r="82%">'
            f'<stop offset="0%" stop-color="{SURFACE}"/>'
            f'<stop offset="100%" stop-color="{BG}"/>'
            "</radialGradient></defs>\n"
            if not compact else ""
        )
        + f'  <rect width="{size}" height="{size}" rx="{radius}" fill="'
        + ('url(#sky)' if not compact else BG)
        + '"/>\n'
        + f'  <g fill="{DIM}">{circles(field, DIM)}</g>\n'
        + star_groups
        + f'  <path fill="{STAR}" d="{a_path}"/>\n'
        + "</svg>\n"
    )


def social_svg(outliner: Outliner) -> str:
    word_paths, _ = outliner.text_paths("Astriena", 500, 300, 95, 2)
    tagline_paths, _ = outliner.text_paths("of the stars", 504, 362, 28, 4)
    descriptor_paths, _ = outliner.text_paths(
        "OpenTelemetry sampling · BYOS ClickHouse", 504, 414, 20
    )
    mark_path = outliner.glyph_path("A", 75, 458, .36)
    paths = "".join(f'<path d="{path}"/>' for path in word_paths)
    tagline = "".join(f'<path d="{path}"/>' for path in tagline_paths)
    descriptor = "".join(f'<path d="{path}"/>' for path in descriptor_paths)
    return (
        svg_header(
            "0 0 1280 640",
            "Astriena — of the stars",
            "Night-sky banner with outlined Cinzel Decorative branding.",
        )
        + '  <defs><radialGradient id="sky" cx="32%" cy="40%" r="80%">'
        f'<stop offset="0%" stop-color="{SURFACE}"/>'
        '<stop offset="55%" stop-color="#0F1828"/>'
        f'<stop offset="100%" stop-color="{BG}"/>'
        "</radialGradient></defs>\n"
        '  <rect width="1280" height="640" fill="url(#sky)"/>\n'
        f'  <g fill="{DIM}">{circles(SOCIAL_FIELD, DIM)}</g>\n'
        f'  <path fill="{STAR}" d="{mark_path}"/>\n'
        f'  <g fill="{STAR}">{paths}</g>\n'
        f'  <g fill="{MUTED}">{tagline}</g>\n'
        f'  <g fill="{MUTED}">{descriptor}</g>\n'
        "</svg>\n"
    )


def rasterize(svg: str, output: Path, width: int, height: int) -> None:
    """Render a generated SVG verbatim; SVG remains the single source of truth."""
    cairosvg.svg2png(
        bytestring=svg.encode(),
        write_to=str(output),
        output_width=width,
        output_height=height,
    )


def main() -> None:
    here = Path(__file__).resolve().parent
    font_path = here / "fonts" / "CinzelDecorative-Regular.ttf"
    outliner = Outliner(font_path)

    mark = mark_svg(outliner)
    compact = mark_svg(outliner, compact=True)
    social = social_svg(outliner)
    (here / "astriena-mark.svg").write_text(mark)
    (here / "astriena-mark-16.svg").write_text(compact)
    (here / "favicon.svg").write_text(compact)
    (here / "astriena-social.svg").write_text(social)

    rasterize(mark, here / "astriena-icon-512.png", 512, 512)
    rasterize(mark, here / "astriena-icon-64.png", 64, 64)
    rasterize(compact, here / "astriena-icon-16.png", 16, 16)
    rasterize(social, here / "astriena-social.png", 1280, 640)


if __name__ == "__main__":
    main()
